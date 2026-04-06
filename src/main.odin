package main

import "core:fmt"
import "core:net"
import "core:strings"
import "core:thread"
import "core:time"
import "core:strconv"

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
    string_content: string,
    
    // list
    content_waiting_room: TicketMutex,
    content: [dynamic] string,
    
    // stream
    entries: [dynamic] Stream_Entry,
    entries_kv: [dynamic] Stream_Key_Value,
}

Stream_Entry :: struct {
    id: Stream_Id,
    kv_start: int,
    kv_count: int,
}

Stream_Id :: struct {
    millis:   int,
    sequence: int,
}

Stream_Key_Value :: struct {
    key:   string,
    value: string,
}

main :: proc (){
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
        fmt.eprintf("full request \n`````\n%v`````\n", client.request)
        
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
            
        case "RPUSH":
            key := parse_key(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List, or_insert = true)
            
            for client.request != "" {
                value := expect_bulk_string(client, "bad value") or_break handle
                
                value_append(list, value)
            }
            
            write_simple_integer(client, value_len(list))
            
        case "LPUSH":
            key := parse_key(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List, or_insert = true)
            
            for client.request != "" {
                value := expect_bulk_string(client, "bad value") or_break handle
                
                // @speed collect then prepend once to avoid multiple copies
                value_prepend(list, value)
            }
            
            write_simple_integer(client, value_len(list))
            
        case "LLEN":
            key := parse_key(client) or_break handle
            
            list, list_ok := store_get(client.store, key, .List)
            write_simple_integer(client, value_len(list))
            
        case "LRANGE":
            key := parse_key(client) or_break handle
            
            start := parse_integer(client) or_break handle
            stop  := parse_integer(client) or_break handle
            
            list, _ := store_get(client.store, key, .List)
            slice := value_slice(list, start, stop)
            
            write_array_of_bulk_string(client, slice)
            
        case "LPOP":
            key := parse_key(client) or_break handle
            
            count := 1
            if client.request != "" {
                count = parse_integer(client) or_break handle
            }
            
            list, list_ok := store_get(client.store, key, .List)
            if value_len(list) == 0 {
                list_ok = false
            }
            
            if count == 1 {
                if list_ok {
                    popped := value_pop(list)
                    write_bulk_string(client, popped)
                } else {
                    write_bulk_string_nil(client)
                }
            } else {
                slice := value_slice(list, 0, count-1)
                write_array_of_bulk_string(client, slice)
                // @cleanup
                for _ in 0..<count do value_pop(list)
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
            timed_out := false
            
            ticket := ticket_mutex_take_ticket(&list.content_waiting_room)
            if value_len(list) == 0 {
                start := time.now()
                block: for {
                    if ticket_mutex_ticket_is_ready(&list.content_waiting_room, ticket) {
                        break block
                    }
                    
                    if time.since(start) > timeout {
                        timed_out = true
                        end_ticket_mutex(&list.content_waiting_room)
                        break block
                    }
                    
                    spin_hint()
                }
            }
            
            if timed_out {
                write_array_nil(client)
            } else {
                popped := value_pop(list)
                write_array_of_bulk_string(client, {key, popped})
            }
            
        case "XADD":
            key := parse_key(client) or_break handle
            
            stream, _ := store_get(client.store, key, .Stream, or_insert = true)
            
            id, ok := chop_bulk_string(&client.request)
            assert(ok)
            
            entry, entry_error := stream_begin_entry(stream, id)
            if entry_error == .none {
                for {
                    item_key   := expect_bulk_string(client, "bad entry key")   or_break handle
                    item_value := expect_bulk_string(client, "bad entry value") or_break handle
                    stream_add_item(stream, entry, item_key, item_value)
                    if client.request == "" do break
                }
                stream_end_entry(stream, entry)
                
                write_sequence_id(client, entry.id)
            } else {
                message: string
                switch entry_error {
                case .none: unreachable()
                case .id_too_smol:   message = "The ID specified in XADD is equal or smaller than the target stream top item"
                case .bad_nil_value: message = "The ID specified in XADD must be greater than 0-0"
                }
                write_simple_error(client, "ERR", message)
            }
            
        case "XRANGE":
            key := parse_key(client) or_break handle
            
            stream, stream_ok := store_get(client.store, key, .Stream)
            if !stream_ok {
                write_simple_error(client, "ERR", "unknown stream")
                break handle
            }
            
            start_id, start_id_ok := chop_bulk_string(&client.request)
            stop_id,  stop_id_ok  := chop_bulk_string(&client.request)
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
            for entry in entries {
                write_array_len(client, 2)
                write_sequence_id(client, entry.id)
                
                write_array_len(client, entry.kv_count*2)
                for kv_index in entry.kv_start..<entry.kv_start+entry.kv_count {
                    kv := stream.entries_kv[kv_index]
                    write_bulk_string(client, kv.key)
                    write_bulk_string(client, kv.value)
                }
            }
            
        case:
            write_simple_error(client, "ERR", "unknown command")
        }
        
        fmt.eprintf("sending response ```\n%v```", strings.to_string(client.response))
        send(client)
    }
}

clone_string :: proc (s: string, allocator := context.allocator) -> string {
    bytes := make([] u8, len(s), allocator)
    copy(bytes, s)
    result := transmute(string) bytes
    return result
}

value_append :: proc (value: ^Value, s: string, loc := #caller_location) {
    assert(value.kind == .List, loc = loc)
    
    s := clone_string(s, context.allocator)
    
    serving := volatile_load(&value.content_waiting_room.serving)
    ticket  := volatile_load(&value.content_waiting_room.ticket)
    if serving < ticket {
        end_ticket_mutex(&value.content_waiting_room)
    }
    
    append(&value.content, s)
}

value_prepend :: proc (value: ^Value, s: string, loc := #caller_location) {
    assert(value.kind == .List, loc = loc)
    
    s := clone_string(s, context.allocator)
    
    serving := volatile_load(&value.content_waiting_room.serving)
    ticket  := volatile_load(&value.content_waiting_room.ticket)
    if serving < ticket {
        end_ticket_mutex(&value.content_waiting_room)
    }
    
    inject_at(&value.content, 0, s)
}

value_pop :: proc (value: ^Value, loc := #caller_location) -> string {
    assert(value.kind == .List, loc = loc)
    
    result := pop_front(&value.content, loc = loc)
    return result
}

value_set :: proc (value: ^Value, s: string, expiration := time.Time{}, loc := #caller_location) {
    assert(value.kind == .String, loc = loc)
    
    value.string_content = s
    value.expiration = expiration
}

value_get :: proc (value: ^Value, loc := #caller_location) -> string {
    assert(value.kind == .String, loc = loc)
    
    result := value.string_content
    return result
}

value_len :: proc (value: ^Value, loc := #caller_location) -> int {
    result: int
    
    if value != nil {
        assert(value.kind == .List)
        result = len(value.content)
    }
    
    return result
}

value_slice :: proc (value: ^Value, start, stop: int, loc := #caller_location) -> [] string {
    assert(value.kind == .List, loc = loc)
    
    start, stop := start, stop
    
    list_ok := value != nil
    if list_ok {
        count := len(value.content)
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
    
    if list_ok && start >= len(value.content) {
        list_ok = false
    }
    
    result: [] string
    if list_ok {
        result = value.content[start:stop+1]
    }
    return result
}

////////////////////////////////////////////////

Id_Error :: enum {
    none,
    id_too_smol,
    bad_nil_value,
}

// @todo(viktor): should the stream be locked by a mutex for the whole operation?
stream_begin_entry :: proc (stream: ^Value, id_string: string, loc := #caller_location) -> (^Stream_Entry, Id_Error) {
    assert(stream.kind == .Stream, loc = loc)
    
    id, id_ok := parse_id(id_string) 
    
    error := Id_Error.none
    if len(stream.entries) > 0 {
        last := stream.entries[len(stream.entries)-1]
        
        if id.millis < last.id.millis {
            error = .id_too_smol
        }
        
        if id.millis == last.id.millis {
            if id.sequence == -1 {
                id.sequence = last.id.sequence + 1
            } else {
                if id.sequence <= last.id.sequence {
                    error = .id_too_smol
                }
            }
        }
    }
    
    if id.sequence == -1 {
        if id.millis == 0 {
            id.sequence = 1
        } else {
            id.sequence = 0
        }
    }
    
    assert(id.millis != -1)
    assert(id.sequence != -1)
    
    if id.millis == 0 && id.sequence == 0 {
        error = .bad_nil_value
    }
    
    result: ^Stream_Entry
    if error == .none {
        count := len(stream.entries)
        append_nothing(&stream.entries)
        result = &stream.entries[count]
        result.id = id
        result.kv_start = len(stream.entries_kv)
    }
    
    return result, error
}

stream_add_item :: proc (stream: ^Value, entry: ^Stream_Entry, key, value: string, loc := #caller_location) {
    assert(stream.kind == .Stream, loc = loc)
    
    key   := clone_string(key, context.allocator)
    value := clone_string(value, context.allocator)
    append(&stream.entries_kv, Stream_Key_Value{key, value})
    entry.kv_count += 1
}

stream_end_entry :: proc (stream: ^Value, entry: ^Stream_Entry, loc := #caller_location) {
    assert(stream.kind == .Stream, loc = loc)
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
    

////////////////////////////////////////////////

send :: proc (client: ^Client) {
    data := strings.to_string(client.response)
    net.send(client.socket, transmute([] u8) data)
    strings.builder_reset(&client.response)
}

////////////////////////////////////////////////

parse_key :: proc (client: ^Client) -> (string, bool) {
    key, key_ok := chop_bulk_string(&client.request)
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
    text, text_ok := chop_bulk_string(&client.request)
    
    result, ok := strconv.parse_int(text)
    if !text_ok || !ok {
        ok = false
        write_simple_error(client, "ERR", "bad number")
    }
    return result, ok
}

parse_float :: proc (client: ^Client) -> (f32, bool) {
    text, text_ok := chop_bulk_string(&client.request)
    
    result, ok := strconv.parse_f32(text)
    if !text_ok || !ok {
        ok = false
        write_simple_error(client, "ERR", "bad number")
    }
    return result, ok
}

expect_bulk_string :: proc (client: ^Client, error_message := "expected bulk string") -> (string, bool) {
    s, ok := chop_bulk_string(&client.request)
    if !ok {
        write_simple_error(client, "ERR", error_message)
    }
    return s, ok
}

chop_bulk_string :: proc (request: ^string) -> (string, bool) {
    line, line_ok := chop(request, "\r\n")
    if !line_ok do return "", false
    
    result, ok := parse_bulk_string(request, &line)
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
