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
}

Store :: struct {
    db: map[string] Value,
    mutex: TicketMutex,
}
    

Value :: struct {
    content: [dynamic] string,
    expiration: time.Time,
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
    
    buffer: [256] u8
    loop: for {
        bytes_read, receive_err := net.recv(client.socket, buffer[:])
        if receive_err != nil {
            fmt.eprintf("%s", receive_err)
            net.close(client.socket)
            break loop
        }
        
        if bytes_read == 0 {
            send_simple_error(client, "ERR", "Empty request")
            break loop
        }
        
        client.request = transmute(string) buffer[:bytes_read]
        fmt.eprintf("full request `%v`\n", client.request)
        line, _ := chop(&client.request, "\r\n")
        count: u64
        if line[0] == '*' {
            line = line[1:]
            ok: bool
            count, ok = strconv.parse_u64(line)
            assert(ok)
        }
        
        for i in 0..<count {
            line, ok := chop(&client.request, "\r\n")
            if !ok do break
            command, command_ok := parse_bulk_string(&client.request, &line)
            assert(command_ok)
            
            command = strings.to_upper(command, context.temp_allocator)
            switch command {
            case "PING":
                send_simple_string(client, "PONG")
                
            case "ECHO":
                content, content_ok := chop_line_and_parse_bulk_string(&client.request)
                assert(content_ok)
                
                send_bulk_string(client, content)
                
            case "SET":
                key := parse_key(client) or_break loop
                
                value, value_ok := chop_line_and_parse_bulk_string(&client.request)
                if !value_ok {
                    send_simple_error(client, "ERR", "bad value")
                    break loop
                }
                
                expiration: time.Time
                if client.request != "" {
                    optional, optional_ok := chop_line_and_parse_bulk_string(&client.request)
                    if !optional_ok {
                        send_simple_error(client, "ERR", "bad optional")
                        break loop
                    }
                    
                    optional = strings.to_upper(optional)
                    switch optional {
                    case "EX":
                        seconds, seconds_ok := chop_line_and_parse_bulk_string(&client.request)
                        if !seconds_ok {
                            send_simple_error(client, "ERR", "bad optional")
                            break loop
                        }
                        
                        secs, secs_ok := strconv.parse_u64(seconds)
                        assert(secs_ok)
                        
                        expiration = time.time_add(time.now(), time.Second * cast(time.Duration) secs)
                    
                    case "PX":
                        milliseconds, milliseconds_ok := chop_line_and_parse_bulk_string(&client.request)
                        if !milliseconds_ok {
                            send_simple_error(client, "ERR", "bad optional")
                            break loop
                        }
                        
                        millis, millis_ok := strconv.parse_u64(milliseconds)
                        assert(millis_ok)
                        
                        expiration = time.time_add(time.now(), time.Millisecond * cast(time.Duration) millis)
                        
                    case:
                        send_simple_error(client, "ERR", "unknown optional")
                        break loop
                    }
                }
                
                the_value, _ := store_get(client.store, key, replace_previous = true)
                fmt.eprintln(the_value, key, expiration)
                value_append(the_value, value)
                the_value.expiration = expiration
                
                send_simple_string(client, "OK")
                
            case "GET":
                key := parse_key(client) or_break loop
                
                value, value_ok := store_get(client.store, key)
                
                if value.expiration != {} {
                    time_left := time.diff(time.now(), value.expiration)
                    if time_left <= 0 {
                        value_ok = false
                    }
                }
                
                if !value_ok {
                    send_bulk_string_nil(client)
                } else {
                    send_bulk_string(client, value.content[0])
                }
                
            case "RPUSH":
                key := parse_key(client) or_break loop
                
                list, list_ok := store_get(client.store, key, or_insert = true)
                
                for client.request != "" {
                    value, value_ok := chop_line_and_parse_bulk_string(&client.request)
                    if !value_ok {
                        send_simple_error(client, "ERR", "bad value")
                        break loop
                    }
                
                    value_append(list, value)
                }
                
                send_simple_integer(client, len(list.content))
                
            case "LPUSH":
                key := parse_key(client) or_break loop
                
                list, list_ok := store_get(client.store, key, or_insert = true)
                
                for client.request != "" {
                    value, value_ok := chop_line_and_parse_bulk_string(&client.request)
                    if !value_ok {
                        send_simple_error(client, "ERR", "bad value")
                        break loop
                    }
                    
                    // @speed collect then prepend once to avoid multiple copies
                    value_prepend(list, value)
                }
                
                send_simple_integer(client, len(list.content))
                
            case "LLEN":
                key := parse_key(client) or_break loop
                
                list, list_ok := store_get(client.store, key)
                send_simple_integer(client, list_ok ? len(list.content) : 0)
                
            case "LRANGE":
                key := parse_key(client) or_break loop
                
                start := parse_integer(client) or_break loop
                stop  := parse_integer(client) or_break loop
                
                list, _ := store_get(client.store, key)
                slice := value_slice(list, start, stop)
                
                send_array_of_bulk_string(client, slice)
                
            case "LPOP":
                key := parse_key(client) or_break loop
                
                count := 1
                if client.request != "" {
                    count = parse_integer(client) or_break loop
                }
                
                list, list_ok := store_get(client.store, key)
                if list_ok {
                    if len(list.content) == 0 {
                        list_ok = false
                    }
                }
                
                if count == 1 {
                    if list_ok {
                        popped := value_pop(list)
                        send_bulk_string(client, popped)
                    } else {
                        send_bulk_string_nil(client)
                    }
                } else {
                    slice := value_slice(list, 0, count-1)
                    send_array_of_bulk_string(client, slice)
                    // @cleanup
                    for _ in 0..<count do value_pop(list)
                }
                
            case "BLPOP":
                key := parse_key(client) or_break loop
                
                timeout := max(time.Duration)
                if client.request != "" {
                    timeout_number := parse_integer(client) or_break loop
                    if timeout_number != 0 {
                        timeout = time.Second * cast(time.Duration) timeout_number
                    }
                }
                
                list, _ := store_get(client.store, key, or_insert = true)
                timed_out := false
                block: for start := time.now(); len(list.content) == 0; {
                    if time.since(start) > timeout {
                        fmt.eprintln("OVER")
                        timed_out = true
                        break block
                    }
                }
                
                if timed_out {
                    fmt.eprintln("TIME OUT")
                    send_array_nil(client)
                } else {
                    popped := value_pop(list)
                    fmt.eprintln("ok", []string{key, popped})
                    send_array_of_bulk_string(client, {key, popped})
                }
                
            case:
                send_simple_error(client, "ERR", "unknown command")
                break loop
            }
        }
    }
}

clone_string :: proc (s: string, allocator := context.allocator) -> string {
    bytes := make([] u8, len(s), allocator)
    copy(bytes, s)
    result := transmute(string) bytes
    return result
}

value_append :: proc (value: ^Value, s: string) {
    s := clone_string(s, context.allocator)
    append(&value.content, s)
}

value_prepend :: proc (value: ^Value, s: string) {
    s := clone_string(s, context.allocator)
    inject_at(&value.content, 0, s)
}

value_pop :: proc (value: ^Value) -> string {
    result := pop_front(&value.content)
    return result
}

store_get :: proc (store: ^Store, key: string, or_insert := false, replace_previous := false) -> (^Value, bool) {
    key := clone_string(key)
    
    begin_ticket_mutex(&store.mutex)
    value, value_ok := &store.db[key]
    end_ticket_mutex(&store.mutex)
    
    make_new := false
    if replace_previous {
        if value_ok do free(value)
        make_new = true
    }
    if or_insert && !value_ok {
        make_new = true
    }
    
    if make_new {
        begin_ticket_mutex(&store.mutex)
        store.db[key] = {}
        value, value_ok = &store.db[key]
        end_ticket_mutex(&store.mutex)
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

value_slice :: proc (value: ^Value, start, stop: int) -> [] string {
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

send_simple_integer :: proc (client: ^Client, data: int) {
    response := fmt.tprintf(":%v\r\n", data)
    send(client, response)
}

send_simple_string :: proc (client: ^Client, data: string) {
    response := fmt.tprintf("+%v\r\n", data)
    send(client, response)
}

send_simple_error :: proc (client: ^Client, error: string, message: string) {
    response := fmt.tprintf("-%v %v\r\n", error, message)
    send(client, response)
    net.close(client.socket)
}

send_array_nil :: proc (client: ^Client) {
    response := "*0\r\n"
    send(client, response)
}

send_array_of_bulk_string :: proc (client: ^Client, array: [] string) {
    if array == nil {
        send_array_nil(client)
    } else {
        response := strings.builder_make(context.temp_allocator)
        fmt.sbprintf(&response, "*%v\r\n", len(array))
        for value in array {
            // @copypasta of format
            fmt.sbprintf(&response, "$%v\r\n%v\r\n", len(value), value)
        }
        
        send(client, strings.to_string(response))
    }
}

send_bulk_string_nil :: proc (client: ^Client) {
    response := "$-1\r\n"
    send(client, response)
}

send_bulk_string :: proc (client: ^Client, data: string) {
    response := fmt.tprintf("$%v\r\n%v\r\n", len(data), data)
    send(client, response)
}

////////////////////////////////////////////////

send :: proc (client: ^Client, data: string) {
    net.send(client.socket, transmute([] u8) data)
}

////////////////////////////////////////////////

parse_key :: proc (client: ^Client) -> (string, bool) {
    key, key_ok := chop_line_and_parse_bulk_string(&client.request)
    if !key_ok {
        send_simple_error(client, "ERR", "missing key")
    }
    return key, key_ok
}

parse_integer :: proc (client: ^Client) -> (int, bool) {
    text, text_ok := chop_line_and_parse_bulk_string(&client.request)
    
    result, ok := strconv.parse_int(text)
    if !text_ok || !ok {
        ok = false
        send_simple_error(client, "ERR", "bad number")
    }
    return result, ok
}

chop_line_and_parse_bulk_string :: proc (request: ^string) -> (string, bool) {
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
