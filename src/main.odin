package main

import "core:fmt"
import "core:net"
import "core:strings"
import "core:thread"
import "core:time"
import "core:strconv"
import "core:sync"

Client :: struct{
    store:   ^Store,
    socket:  net.TCP_Socket,
    request: string,
    response: strings.Builder,
}

Store :: struct {
    db: map[string] Value,
    mutex: TicketMutex,
}
    

Value_Kind :: enum {
    None,
    String, 
    List,
    Set,
    ZSet,
    Hash,
    Stream,
    VectorSet,
}

Value_Kind_String := [Value_Kind] string {
    .None      = "none",
    .String    = "string",
    .List      = "list",
    .Set       = "set",
    .ZSet      = "zset",
    .Hash      = "hash",
    .Stream    = "stream",
    .VectorSet = "vectorset",
}

Value :: struct {
    kind: Value_Kind,
    expiration: time.Time,
    
    // string
    content: string,
    
    // list
    items_semaphore: sync.Sema,
    items: [dynamic] string,
    
    // stream
    entries: [dynamic] Stream_Entry,
    
    // sorted set
    members_score:   [dynamic] f32,
}

main :: proc () {
    pool: thread.Pool
    thread.pool_init(&pool, context.allocator, 11)
    thread.pool_start(&pool)
    
    listen_socket, listen_err := net.listen_tcp(net.Endpoint{ port = 6379, address = net.IP4_Loopback })
    if listen_err != nil {
        fmt.panicf("%s", listen_err)
    }
    
    store: Store
    
    for {
        client_socket, client_endpoint, accept_err := net.accept_tcp(listen_socket)
        if accept_err != nil {
            fmt.panicf("%s", accept_err)
        }
        
        client := new(Client)
        client.socket = client_socket
        client.store = &store
        thread.pool_add_task(&pool, context.allocator, handle_client, client)
    }
}

handle_client :: proc (task: thread.Task) {
    client := cast(^Client) task.data
    defer free(client)
    
    client.response = strings.builder_make(context.allocator)
    defer strings.builder_destroy(&client.response)
    
    defer net.close(client.socket)
    
    buffer: [2048] u8
    for {
        bytes_read, receive_err := net.recv(client.socket, buffer[:])
        if receive_err == .Connection_Closed do break
        if receive_err != nil {
            fmt.eprintf("Receive error: %s\n", receive_err)
            return
        }
        
        if bytes_read == 0 {
            write_simple_error(client, "ERR", "Empty request")
            send(client)
            return
        }
        
        client.request = transmute(string) buffer[:bytes_read]
        // fmt.eprintf("full request \n`````\n%v`````\n", client.request)
        
        // @todo(viktor): we could preparse/presplit based on count
        line, _ := chop(&client.request, "\r\n")
        count: u64
        if line[0] == '*' {
            line = line[1:]
            ok: bool
            count, ok = strconv.parse_u64(line)
            assert(ok)
        }
        
        ok: bool
        line, ok = chop(&client.request, "\r\n")
        assert(ok)
        command, command_ok := parse_bulk_string(&client.request, &line)
        assert(command_ok)
            
        command = strings.to_upper(command, context.temp_allocator)
        
        handle: switch command {
        case "PING":
            write_simple_string(client, "PONG")
            
        case "ECHO":
            message := expect_bulk_string(client) or_break handle
            
            write_bulk_string(client, message)
            
        case "TYPE":
            key := parse_key(client) or_break handle
            
            type := store_type(client.store, key)
            
            write_simple_string(client, Value_Kind_String[type])
            
        ////////////////////////////////////////////////
            
        case "SET":
            key := parse_key(client) or_break handle
            
            content := expect_bulk_string(client, "bad value") or_break handle
            
            expiration: time.Time
            if client.request != "" {
                optional := expect_bulk_string(client, "bad optional") or_break handle
                optional = strings.to_upper(optional)
                
                switch optional {
                case "EX":
                    seconds := parse_float(client) or_break handle
                    expiration = time.time_add(time.now(), cast(time.Duration) (cast(f32) time.Second * seconds))
                
                case "PX":
                    milliseconds := parse_float(client) or_break handle
                    expiration = time.time_add(time.now(), cast(time.Duration) (cast(f32) time.Millisecond * milliseconds))
                    
                case:
                    write_simple_error(client, "ERR", "unknown optional")
                    break handle
                }
            }
            
            the_value, _ := store_get(client.store, key, .String, replace_previous = true)
            value_set(the_value, content, expiration = expiration)
            
            write_simple_string(client, "OK")
            
        case "GET":
            key := parse_key(client) or_break handle
            
            value, value_ok := store_get(client.store, key, .String)
            
            if !value_ok {
                write_bulk_string_nil(client)
            } else {
                write_bulk_string(client, value_get(value))
            }
            
        ////////////////////////////////////////////////
            
        case "RPUSH":
            key := parse_key(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List, or_insert = true)
            
            for client.request != "" {
                value := expect_bulk_string(client, "bad value") or_break handle
                
                list_append(list, value)
            }
            
            write_simple_integer(client, list_len(list))
            
        case "LPUSH":
            key := parse_key(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List, or_insert = true)
            
            for client.request != "" {
                value := expect_bulk_string(client, "bad value") or_break handle
                
                // @speed collect then prepend once to avoid multiple copies
                list_prepend(list, value)
            }
            
            write_simple_integer(client, list_len(list))
            
        case "LLEN":
            key := parse_key(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List)
            write_simple_integer(client, list_len(list))
            
        case "LRANGE":
            key := parse_key(client) or_break handle
            
            start := parse_integer(client) or_break handle
            stop  := parse_integer(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List)
            if !list_ok {
                write_array_len(client, 0)
                break handle
            }
            
            slice := list_slice(list, start, stop)
            write_array_of_bulk_string(client, slice)
            
        case "LPOP":
            key := parse_key(client) or_break handle
            
            count := 1
            if client.request != "" {
                count = parse_integer(client) or_break handle
            }
            
            list, list_ok := store_get(client.store, key, .List)
            if list_len(list) == 0 {
                list_ok = false
            }
            
            if count == 1 {
                if list_ok {
                    popped := list_pop(list)
                    write_bulk_string(client, popped)
                } else {
                    write_bulk_string_nil(client)
                }
            } else {
                slice := list_slice(list, 0, count-1)
                write_array_of_bulk_string(client, slice)
                // @cleanup
                for _ in 0..<count do list_pop(list)
            }
            
        case "BLPOP":
            key := parse_key(client) or_break handle
            
            timeout := max(time.Duration)
            if client.request != "" {
                timeout_number := parse_float(client) or_break handle
                if timeout_number != 0 {
                    timeout = cast(time.Duration) (cast(f32) time.Second * timeout_number)
                }
            }
            
            list, _ := store_get(client.store, key, .List, or_insert = true)
            
            if !list_block(list, timeout) {
                write_array_nil(client)
                break handle
            }
            
            if len(list.items) == 0 {
                write_simple_error(client, "ERR", "internal blocking error")
                break handle
            }
            
            popped := list_pop(list)
            write_array_of_bulk_string(client, {key, popped})
    
        ////////////////////////////////////////////////
            
        case "XADD":
            key := parse_key(client) or_break handle
            
            stream, _ := store_get(client.store, key, .Stream, or_insert = true)
            
            id, ok := chop_bulk_string(client)
            assert(ok)
            
            entry, entry_error := stream_begin_entry(stream, id)
            if entry_error != .none {
                message: string
                switch entry_error {
                case .none: unreachable()
                case .id_too_smol:   message = "The ID specified in XADD is equal or smaller than the target stream top item"
                case .bad_nil_value: message = "The ID specified in XADD must be greater than 0-0"
                }
                write_simple_error(client, "ERR", message)
                break handle
            }
            
            for {
                item_key   := expect_bulk_string(client, "bad entry key")   or_break handle
                item_value := expect_bulk_string(client, "bad entry value") or_break handle
                stream_add_item(stream, entry, item_key, item_value)
                if client.request == "" do break
            }
            stream_end_entry(stream, entry)
            
            write_sequence_id(client, entry.id)
            
        case "XRANGE":
            key := parse_key(client) or_break handle
            
            stream, stream_ok := store_get(client.store, key, .Stream)
            if !stream_ok {
                write_simple_error(client, "ERR", "unknown stream")
                break handle
            }
            
            start_id, start_id_ok := chop_bulk_string(client)
            stop_id,  stop_id_ok  := chop_bulk_string(client)
            if !start_id_ok || !stop_id_ok {
                write_simple_error(client, "ERR", "bad start stop")
                break handle
            }
            
            start, start_ok := parse_id_or_first_id(stream, start_id, 0)
            stop,  stop_ok  := parse_id_or_last_id(stream, stop_id, max(int))
            if !start_ok || !stop_ok {
                write_simple_error(client, "ERR", "bad id")
                break handle
            }
            
            entry_start, entry_stop := len(stream.entries), len(stream.entries)
            find: for entry, entry_index in stream.entries {
                if entry_start == len(stream.entries) {
                    if entry.id.millis >= start.millis && entry.id.sequence >= start.sequence {
                        entry_start = entry_index
                    }
                } else {
                    if entry.id.millis > stop.millis || entry.id.sequence > stop.sequence {
                        entry_stop = entry_index
                        break find
                    }
                }
            }
            
            entries := stream.entries[entry_start:entry_stop]
            write_array_len(client, len(entries))
            for &entry in entries {
                write_stream_entry(client, stream, &entry)
            }
            
        case "XREAD":
            streams, streams_ok := chop_bulk_string(client)
            if !streams_ok || streams != "streams" {
                write_simple_error(client, "ERR", "missing 'streams'")
                break handle
            }
            
            key := parse_key(client) or_break handle
            
            stream, stream_ok := store_get(client.store, key, .Stream)
            if !stream_ok {
                write_simple_error(client, "ERR", "unknown stream")
                break handle
            }
            
            id_string, id_string_ok := chop_bulk_string(client)
            if !id_string_ok {
                write_simple_error(client, "ERR", "bad id")
                break handle
            }
            
            start, start_ok := parse_id(id_string)
            if !start_ok {
                write_simple_error(client, "ERR", "bad id")
                break handle
            }
            
            
            entry_start := 0
            find_read: for entry, entry_index in stream.entries {
                if entry.id.millis > start.millis && entry.id.sequence > start.sequence {
                    entry_start = entry_index
                    break find_read
                }
            }
            
            entries := stream.entries[entry_start:]
            write_array_len(client, 1)
            write_array_len(client, 2)
            write_bulk_string(client, key)
            write_array_len(client, len(entries))
            for &entry in entries {
                write_stream_entry(client, stream, &entry)
            }
            
        ////////////////////////////////////////////////
        
        case "ZADD":
            key := parse_key(client) or_break handle
            
            set, _ := store_get(client.store, key, .ZSet, or_insert = true)
            
            score := parse_float(client) or_break handle
            value := chop_bulk_string(client) or_break handle
            
            added_count := set_add(set, score, value)
            write_simple_integer(client, added_count)
        
        case "ZRANK":
            key := parse_key(client) or_break handle
            
            set, set_ok := store_get(client.store, key, .ZSet)
            if !set_ok {
                write_bulk_string_nil(client)
                break handle
            }
            
            fmt.eprintf("items = %v\n", set.items)
            fmt.eprintf("score = %v\n", set.members_score)
            
            value := chop_bulk_string(client) or_break handle
            
            rank, ok := set_rank(set, value)
            if !ok {
                write_bulk_string_nil(client)
            } else {
                write_simple_integer(client, rank)
            }
                
        case "ZRANGE":
            key := parse_key(client) or_break handle
            
            start := parse_integer(client) or_break handle
            stop  := parse_integer(client) or_break handle
            
            set, set_ok := store_get(client.store, key, .ZSet)
            if !set_ok {
                write_array_len(client, 0)
                break handle
            }
            
            slice := set_slice(set, start, stop)
            write_array_of_bulk_string(client, slice)
            
        ////////////////////////////////////////////////
            
        case:
            write_simple_error(client, "ERR", "unknown command")
        }
        
        fmt.eprintf("sending response ```\n%v```", strings.to_string(client.response))
        send(client)
    }
}

////////////////////////////////////////////////

set_add :: proc (set: ^Value, score: f32, value: string, loc := #caller_location) -> int {
    assert(set.kind == .ZSet, loc = loc)
    
    // @speed hash lookup
    do_add := true
    is_update := false
    for it, it_index in set.items {
        if it == value {
            it_score := set.members_score[it_index]
            if it_score < score {
                do_add = false
            } else {
                is_update = true
                ordered_remove(&set.items, it_index)
            }
            break
        }
    }
    
    result: int
    if do_add {
        index := len(set.items)
        search: for it_score, score_index in set.members_score {
            if it_score == score {
                it_content := set.items[score_index]
                if value < it_content {
                    index = score_index
                } else {
                    index = score_index+1
                }
                break search
            }
            
            if it_score > score {
                index = score_index
                break search
            }
        }
        
        value_add_item(set, value, index)
        inject_at(&set.members_score, index, score)
        if !is_update {
            result += 1
        }
    }
    
    return result
}

set_rank :: proc (set: ^Value, value: string, loc := #caller_location) -> (int, bool) {
    assert(set.kind == .ZSet, loc = loc)
    
    result: int
    ok: bool
    
    search: for it, it_index in set.items {
        if it == value {
            result = it_index
            ok = true
            break search
        }
    }
    
    return result, ok
}

set_slice :: proc (set: ^Value, start, stop: int, loc := #caller_location) -> [] string {
    assert(set.kind == .ZSet, loc = loc)
    
    result := value_slice(set, start, stop)
    return result
}

clone_string :: proc (s: string, allocator := context.allocator) -> string {
    bytes := make([] u8, len(s), allocator)
    copy(bytes, s)
    result := transmute(string) bytes
    return result
}

value_add_item :: proc (value: ^Value, s: string, index: int) {
    s := clone_string(s, context.allocator)
    
    inject_at(&value.items, index, s)
    sync.sema_post(&value.items_semaphore, 1)
}

value_slice :: proc (value: ^Value, start, stop: int) -> [] string {
    start, stop := start, stop
    
    list_ok := value != nil
    if list_ok {
        count := len(value.items)
        if start < 0 {
            if start < -count {
                start = 0 
            } else {
                start = ((start % count) + count) % count
            }
        }
        
        if stop < 0 {
            stop = ((stop % count) + count) % count
        } else if stop > count {
            stop = count-1
        }
    }
        
    if start > stop {
        list_ok = false
    }
    
    if list_ok && start >= len(value.items) {
        list_ok = false
    }
    
    result: [] string
    if list_ok {
        result = value.items[start:stop+1]
    }
    return result
}

value_set :: proc (value: ^Value, s: string, expiration := time.Time{}, loc := #caller_location) {
    assert(value.kind == .String, loc = loc)
    
    value.content = s
    value.expiration = expiration
}

value_get :: proc (value: ^Value, loc := #caller_location) -> string {
    assert(value.kind == .String, loc = loc)
    
    result := value.content
    return result
}

////////////////////////////////////////////////

store_type :: proc (store: ^Store, key: string) -> Value_Kind {
    begin_ticket_mutex(&store.mutex)
    value, value_ok := &store.db[key]
    end_ticket_mutex(&store.mutex)
    
    result: Value_Kind
    if value_ok {
        result = value.kind
    }
    return result
}

store_get :: proc (store: ^Store, key: string, kind: Value_Kind, or_insert := false, replace_previous := false) -> (^Value, bool) {
    key := clone_string(key)
    
    begin_ticket_mutex(&store.mutex)
    value, value_ok := &store.db[key]
    end_ticket_mutex(&store.mutex)
    
    make_new := false
    if replace_previous {
        make_new = true
        free(value)
    }
    if or_insert && !value_ok {
        make_new = true
    }
    
    if make_new {
        begin_ticket_mutex(&store.mutex)
        store.db[key] = {
            kind = kind,
        }
        value, value_ok = &store.db[key]
        end_ticket_mutex(&store.mutex)
    }
    
    if value_ok {
        if value.kind != kind {
            value_ok = false
        }
    }
    
    if value_ok {
        if value.expiration != {} {
            time_left := time.diff(time.now(), value.expiration)
            if time_left <= 0 {
                value_ok = false
            }
        }
    }
    
    return value, value_ok
}

////////////////////////////////////////////////

write_simple_integer :: proc (client: ^Client, data: int) {
    fmt.sbprintf(&client.response, ":%v\r\n", data)
}

write_simple_string :: proc (client: ^Client, data: string) {
    fmt.sbprintf(&client.response, "+%v\r\n", data)
}

write_simple_error :: proc (client: ^Client, error: string, message: string) {
    fmt.sbprintf(&client.response, "-%v %v\r\n", error, message)
}

write_array_nil :: proc (client: ^Client) {
    fmt.sbprint(&client.response, "*-1\r\n")
}

write_bulk_string_nil :: proc (client: ^Client) {
    fmt.sbprint(&client.response, "$-1\r\n")
}

write_array_len :: proc (client: ^Client, array_len: int) {
    fmt.sbprintf(&client.response, "*%v\r\n", array_len)
}

write_array_of_bulk_string :: proc (client: ^Client, array: [] string) {
    write_array_len(client, len(array))
    for value in array {
        write_bulk_string(client, value)
    }
}

write_bulk_string :: proc (client: ^Client, data: string) {
    fmt.sbprintf(&client.response, "$%v\r\n%v\r\n", len(data), data)
}

////////////////////////////////////////////////

write_sequence_id :: proc (client: ^Client, id: Stream_Id) {
    write_bulk_string(client, fmt.tprintf("%v-%v", id.millis, id.sequence))
}

write_stream_entry :: proc (client: ^Client, stream: ^Value, entry: ^Stream_Entry) {
    assert(stream.kind == .Stream)
    
    write_array_len(client, 2)
    write_sequence_id(client, entry.id)
    
    entries := stream.items[entry.kv_start:][:entry.kv_count*2]
    write_array_of_bulk_string(client, entries)
}

////////////////////////////////////////////////

send :: proc (client: ^Client) {
    data := strings.to_string(client.response)
    net.send(client.socket, transmute([] u8) data)
    strings.builder_reset(&client.response)
}

////////////////////////////////////////////////

parse_key :: proc (client: ^Client) -> (string, bool) {
    key, key_ok := chop_bulk_string(client)
    if !key_ok {
        write_simple_error(client, "ERR", "missing key")
    }
    return key, key_ok
}

parse_id_or_first_id :: proc (stream: ^Value, id: string, default_sequence := -1) -> (Stream_Id, bool) {
    assert(stream.kind == .Stream)
    
    result: Stream_Id
    ok: bool
    if id == "-" {
        if len(stream.entries) > 0 {
            result = stream.entries[0].id
            ok = true
        }
    } else {
        result, ok = parse_id(id, default_sequence)
    }
    
    return result, ok
}

parse_id_or_last_id :: proc (stream: ^Value, id: string, default_sequence := -1) -> (Stream_Id, bool) {
    assert(stream.kind == .Stream)
    
    result: Stream_Id
    ok: bool
    if id == "+" {
        if len(stream.entries) > 0 {
            result = stream.entries[len(stream.entries)-1].id
            ok = true
        }
    } else {
        result, ok = parse_id(id, default_sequence)
    }
    
    return result, ok
}

parse_id :: proc (id: string, default_sequence := -1) -> (Stream_Id, bool) {
    result := Stream_Id { -1, default_sequence }
    parse_millis   := false
    parse_sequence := false
    if id == "*" {
        result.millis = cast(int) (time.diff(time.Time{}, time.now()) / time.Millisecond)
    } else if strings.ends_with(id, "*") {
        parse_millis = true
    } else {
        parse_millis   = true
        parse_sequence = true
    }
    
    id := id
    if parse_millis {
        milli_string, ok := chop(&id, "-")
        if !ok {
            milli_string = id
            parse_sequence = false
        }
        
        result.millis, ok = strconv.parse_int(milli_string)
        assert(ok)
    }
    
    if parse_sequence {
        ok: bool
        result.sequence, ok = strconv.parse_int(id)
        assert(ok)
    }
    
    ok := result.millis != -1
    
    return result, ok
}

parse_integer :: proc (client: ^Client) -> (int, bool) {
    text, text_ok := chop_bulk_string(client)
    
    result, ok := strconv.parse_int(text)
    if !text_ok || !ok {
        ok = false
        write_simple_error(client, "ERR", "bad number")
    }
    return result, ok
}

parse_float :: proc (client: ^Client) -> (f32, bool) {
    text, text_ok := chop_bulk_string(client)
    
    result, ok := strconv.parse_f32(text)
    if !text_ok || !ok {
        ok = false
        write_simple_error(client, "ERR", "bad number")
    }
    return result, ok
}

expect_bulk_string :: proc (client: ^Client, error_message := "expected bulk string") -> (string, bool) {
    s, ok := chop_bulk_string(client)
    if !ok {
        write_simple_error(client, "ERR", error_message)
    }
    return s, ok
}

chop_bulk_string :: proc (client: ^Client) -> (string, bool) {
    line, line_ok := chop(&client.request, "\r\n")
    if !line_ok do return "", false
    
    result, ok := parse_bulk_string(&client.request, &line)
    return result, ok
}

parse_bulk_string :: proc (request: ^string, line: ^string) -> (string, bool) {
    length: u64
    ok: bool
    if line[0] == '$' {
        line^ = line[1:]
        length, ok = strconv.parse_u64(line^)
    }
    
    result: string
    if ok {
        result  = request[:length]
        request^ = request[length+len("\r\n"):]
    }
    
    return result, ok
}

chop :: proc (s: ^string, until: string) -> (string, bool) {
    head, match, tail := strings.partition(s^, until)
    ok := len(head) != 0
    s^ = tail
    return head, ok
}
