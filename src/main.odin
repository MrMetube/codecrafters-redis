package main

import "core:fmt"
import "core:math"
import "core:net"
import "core:strings"
import "core:strconv"
import "core:sync"
import "core:thread"
import "core:time"

Client :: struct{
    store:    ^Store,
    
    socket:   net.TCP_Socket,
    request:  string,
    response: strings.Builder,
}

Store :: struct {
    db:    map[string] Value,
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
    members_score: [dynamic] f64,
}

Command :: union {
    // Ping,
    // Echo,
    // Type,
    Set,
    // Get,
    Incr,
    
    // RPush,
    // LPush,
    // LLen,
    // LRange,
    // LPop,
    // BLPop,
    
    // XAdd,
    // XRange,
    // XRead,
    
    // ZAdd,
    // ZRem,
    // ZRank,
    // ZScore,
    // ZRange,
    // ZCard,
}

Set :: struct {
    key:   string,
    value: string,
    expiration: time.Time,
}

Incr :: struct {
    key: string,
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
    inside_multi: bool
    did_exec: bool
    commands: [dynamic] Command
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
                optional = strings.to_upper(optional, context.temp_allocator)
                
                factor: time.Duration
                switch optional {
                case "EX": factor = time.Second
                case "PX": factor = time.Millisecond
                    
                case:
                    write_simple_error(client, "ERR", "unknown optional")
                    break handle
                }
                
                time_amount := parse_float(client) or_break handle
                duration    := cast(time.Duration) (cast(f64) factor * time_amount)
                expiration   = time.time_add(time.now(), duration)
            }
            
            append(&commands, Set{ clone_string(key), clone_string(content), expiration })
            
        case "GET":
            key := parse_key(client) or_break handle
            
            value, value_ok := store_get(client.store, key, .String)
            
            if !value_ok {
                write_bulk_string_nil(client)
            } else {
                write_bulk_string(client, value_get(value))
            }
            
        case "INCR":
            key := parse_key(client) or_break handle
            append(&commands, Incr{ clone_string(key) })
            
        ////////////////////////////////////////////////
        
        case "MULTI":
            inside_multi = true
            
        case "EXEC":
            if inside_multi {
                inside_multi = false
            } else {
                write_simple_error(client, "ERR", "EXEC without MULTI")
            }
        
        ////////////////////////////////////////////////
            
        case "RPUSH":
            key := parse_key(client) or_break handle
            
            list, _ := store_get(client.store, key, .List, or_insert = true)
            
            for client.request != "" {
                value := expect_bulk_string(client, "bad value") or_break handle
                
                list_append(list, value)
            }
            
            write_simple_integer(client, list_len(list^))
            
        case "LPUSH":
            key := parse_key(client) or_break handle
            
            list, _ := store_get(client.store, key, .List, or_insert = true)
            
            for client.request != "" {
                value := expect_bulk_string(client, "bad value") or_break handle
                
                // @speed collect then prepend once to avoid multiple copies
                list_prepend(list, value)
            }
            
            write_simple_integer(client, list_len(list^))
            
        case "LLEN":
            key := parse_key(client) or_break handle
            
            list, ok := store_get(client.store, key, .List)
            count: int
            if ok {
                count = list_len(list^)
            }
            write_simple_integer(client, count)
            
        case "LRANGE":
            key := parse_key(client) or_break handle
            
            start := parse_integer(client) or_break handle
            stop  := parse_integer(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List)
            if !list_ok {
                write_array_len(client, 0)
                break handle
            }
            
            slice := list_slice(list^, start, stop)
            write_array_of_bulk_string(client, slice)
            
        case "LPOP":
            key := parse_key(client) or_break handle
            
            count := 1
            if client.request != "" {
                count = parse_integer(client) or_break handle
            }
            
            list, list_ok := store_get(client.store, key, .List)
            if list_ok && list_len(list^) == 0 {
                list_ok = false
            }
            
            if !list_ok {
                write_bulk_string_nil(client)
                break handle
            }
            
            if count == 1 {
                popped := list_pop(list)
                write_bulk_string(client, popped)
            } else {
                slice := list_slice(list^, 0, count-1)
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
                    timeout = cast(time.Duration) (cast(f64) time.Second * timeout_number)
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
            
            id := expect_bulk_string(client, "missing id") or_break handle
            
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
            
            start_id := expect_bulk_string(client, "bad start") or_break handle
            stop_id  := expect_bulk_string(client, "bad stop")  or_break handle
            
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
            streams := expect_bulk_string(client, "missing 'streams'") or_break handle
            if streams != "streams" {
                write_simple_error(client, "ERR", "expected 'streams'")
                break handle
            }
            
            key := parse_key(client) or_break handle
            
            stream, stream_ok := store_get(client.store, key, .Stream)
            if !stream_ok {
                write_simple_error(client, "ERR", "unknown stream")
                break handle
            }
            
            id_string := expect_bulk_string(client) or_break handle
            
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
            
            zset, _ := store_get(client.store, key, .ZSet, or_insert = true)
            
            score := parse_float(client) or_break handle
            value := expect_bulk_string(client, "bad value") or_break handle
            
            added_count := zset_add(zset, score, value)
            write_simple_integer(client, added_count)
            
        case "ZREM":
            key := parse_key(client) or_break handle
            
            zset, zset_ok := store_get(client.store, key, .ZSet)
            if !zset_ok {
                write_simple_integer(client, 0)
                break handle
            }
            
            value := expect_bulk_string(client, "bad value") or_break handle
            
            removed_count := zset_remove(zset, value)
            write_simple_integer(client, removed_count)
        
        case "ZRANK":
            key := parse_key(client) or_break handle
            
            zset, zset_ok := store_get_readonly(client.store, key, .ZSet)
            if !zset_ok {
                write_bulk_string_nil(client)
                break handle
            }
            
            value := expect_bulk_string(client, "bad value") or_break handle
            
            rank, ok := zset_rank(zset, value)
            if !ok {
                write_bulk_string_nil(client)
            } else {
                write_simple_integer(client, rank)
            }
            
        case "ZSCORE":
            key := parse_key(client) or_break handle
            
            zset, zset_ok := store_get_readonly(client.store, key, .ZSet)
            if !zset_ok {
                write_bulk_string_nil(client)
                break handle
            }
            
            value := expect_bulk_string(client, "bad value") or_break handle
            
            score, ok := zset_score(zset, value)
            if !ok {
                write_bulk_string_nil(client)
            } else {
                write_simple_float(client, score)
            }
                
        case "ZRANGE":
            key := parse_key(client) or_break handle
            
            start := parse_integer(client) or_break handle
            stop  := parse_integer(client) or_break handle
            
            zset, zset_ok := store_get_readonly(client.store, key, .ZSet)
            if !zset_ok {
                write_array_len(client, 0)
                break handle
            }
            
            slice := zset_slice(zset, start, stop)
            write_array_of_bulk_string(client, slice)
            
        case "ZCARD":
            key := parse_key(client) or_break handle
            
            zset, ok := store_get_readonly(client.store, key, .ZSet)
            cardinality: int
            if ok {
                cardinality = zset_card(zset)
            }
            write_simple_integer(client, cardinality)
            
        ////////////////////////////////////////////////
            
        case:
            write_simple_error(client, "ERR", "unknown command")
        }
        
        if inside_multi {
            write_simple_string(client, "OK")
        } else {
            if did_exec {
                did_exec = false
                write_array_len(client, len(commands))
            }
            
            for command in commands {
                switch cmd in command {
                case Set:
                    value, _ := store_get(client.store, cmd.key, .String, replace_previous = true)
                    value_set(value, cmd.value, expiration = cmd.expiration)
                    write_simple_string(client, "OK")
                    
                case Incr:
                    value, _ := store_get(client.store, cmd.key, .String, or_insert = true)
                    
                    content := value_get(value)
                    if content == "" {
                        value_set(value, "0")
                        content = value_get(value)
                    }
                    
                    number, ok := strconv.parse_int(content)
                    
                    if ok {
                        number += 1
                        value_set(value, fmt.aprintf("%v", number, allocator = context.allocator))
                        write_simple_integer(client, number)
                    } else {
                        write_simple_error(client, "ERR", "value is not an integer or out of range")
                    }
                }
            }
            
            clear(&commands)
        }
        
        fmt.eprintf("sending response ```\n%v```", strings.to_string(client.response))
        send(client)
    }
}

////////////////////////////////////////////////

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

value_slice :: proc (value: Value, start, stop: int) -> [] string {
    start, stop := start, stop
    
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
        
    ok := true
    if start > stop {
        ok = false
    }
    
    if ok && start >= count {
        ok = false
    }
    
    result: [] string
    if ok {
        result = value.items[start:stop+1]
    }
    return result
}

value_set :: proc (value: ^Value, s: string, expiration := Maybe(time.Time){}, loc := #caller_location) {
    assert(value.kind == .String, loc = loc)
    
    value.content = s
    if exp, ok := expiration.?; ok {
        value.expiration = exp
    }
}

value_get :: proc (value: ^Value, loc := #caller_location) -> string {
    assert(value.kind == .String, loc = loc)
    
    result := value.content
    return result
}

////////////////////////////////////////////////

store_type :: proc (store: ^Store, key: string) -> Value_Kind {
    begin_ticket_mutex(&store.mutex)
    value, value_ok := store.db[key]
    end_ticket_mutex(&store.mutex)
    
    result: Value_Kind
    if value_ok {
        result = value.kind
    }
    return result
}

store_get_readonly :: proc (store: ^Store, key: string, kind: Value_Kind, or_insert := false, replace_previous := false) -> (Value, bool) {
    value, ok := store_get(store, key, kind, or_insert, replace_previous)
    result: Value
    if ok {
        result = value^
    }
    return result, ok
}

store_get :: proc (store: ^Store, key: string, kind: Value_Kind, or_insert := false, replace_previous := false) -> (^Value, bool) {
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
        key := clone_string(key)
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

write_simple_float :: proc (client: ^Client, data: f64) {
    write_bulk_string(client, fmt.tprintf("%.*f", math.MAX_F64_PRECISION, data))
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
    key, ok := expect_bulk_string(client, "missing key")
    return key, ok
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

parse_float :: proc (client: ^Client) -> (f64, bool) {
    text, text_ok := chop_bulk_string(client)
    
    result, ok := strconv.parse_f64(text)
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
