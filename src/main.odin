package main

import "core:fmt"
import "core:net"
import "core:strings"
import "core:thread"
import "core:time"
import "core:strconv"

Client :: struct{
    socket: net.TCP_Socket,
}

main :: proc (){
    pool: thread.Pool
    thread.pool_init(&pool, context.allocator, 11)
    thread.pool_start(&pool)
    
    listen_socket, listen_err := net.listen_tcp(net.Endpoint{ port = 6379, address = net.IP4_Loopback })
    if listen_err != nil {
        fmt.panicf("%s", listen_err)
    }
    
    for {
        client_socket, client_endpoint, accept_err := net.accept_tcp(listen_socket)
        if accept_err != nil {
            fmt.panicf("%s", accept_err)
        }
        
        client := new(Client)
        client.socket = client_socket
        thread.pool_add_task(&pool, context.allocator, handle_client, client)
    }
}

Store :: map[string] Value

Value :: struct {
    content: [dynamic] string,
    expiration: time.Time,
}

handle_client :: proc (task: thread.Task) {
    client := cast(^Client) task.data
    
    store: Store
    
    buffer: [256] u8
    loop: for {
        bytes_read, receive_err := net.recv(client.socket, buffer[:])
        if receive_err != nil {
            fmt.panicf("%s", receive_err)
        }
        
        if bytes_read == 0 {
            send_simple_error(client, "ERR", "Empty request")
            break loop
        }
        
        request := transmute(string) buffer[:bytes_read]
        fmt.eprintf("full request `%v`\n", request)
        line, _ := chop(&request, "\r\n")
        count: u64
        if line[0] == '*' {
            line = line[1:]
            ok: bool
            count, ok = strconv.parse_u64(line)
            assert(ok)
        }
        
        for i in 0..<count {
            line, ok := chop(&request, "\r\n")
            if !ok do break
            command, command_ok := parse_bulk_string(&request, &line)
            assert(command_ok)
            
            command = strings.to_upper(command, context.temp_allocator)
            switch command {
            case "PING":
                send_simple_string(client, "PONG")
                
            case "ECHO":
                content, content_ok := chop_line_and_parse_bulk_string(&request)
                assert(content_ok)
                
                send_bulk_string(client, content)
                
            case "SET":
                key, key_ok := chop_line_and_parse_bulk_string(&request)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    break loop
                }
                
                value, value_ok := chop_line_and_parse_bulk_string(&request)
                if !value_ok {
                    send_simple_error(client, "ERR", "bad value")
                    break loop
                }
                
                expiration: time.Time
                if request != "" {
                    optional, optional_ok := chop_line_and_parse_bulk_string(&request)
                    if !optional_ok {
                        send_simple_error(client, "ERR", "bad optional")
                        break loop
                    }
                    
                    optional = strings.to_upper(optional)
                    switch optional {
                    case "EX":
                        seconds, seconds_ok := chop_line_and_parse_bulk_string(&request)
                        if !seconds_ok {
                            send_simple_error(client, "ERR", "bad optional")
                            break loop
                        }
                        
                        secs, secs_ok := strconv.parse_u64(seconds)
                        assert(secs_ok)
                        
                        expiration = time.time_add(time.now(), time.Second * cast(time.Duration) secs)
                    
                    case "PX":
                        milliseconds, milliseconds_ok := chop_line_and_parse_bulk_string(&request)
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
                
                the_value, _ := store_get(&store, key, replace_previous = true)
                fmt.eprintln(the_value, key, expiration)
                value_append(the_value, value)
                the_value.expiration = expiration
                
                send_simple_string(client, "OK")
                
            case "GET":
                key, key_ok := chop_line_and_parse_bulk_string(&request)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    break loop
                }
                
                value, value_ok := store_get(&store, key)
                
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
                key, key_ok := chop_line_and_parse_bulk_string(&request)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    break loop
                }
                
                list, list_ok := store_get(&store, key, or_insert = true)
                
                for request != "" {
                    value, value_ok := chop_line_and_parse_bulk_string(&request)
                    if !value_ok {
                        send_simple_error(client, "ERR", "bad value")
                        break loop
                    }
                
                    value_append(list, value)
                }
                
                send_simple_integer(client, len(list.content))
                
            case "LPUSH":
                key, key_ok := chop_line_and_parse_bulk_string(&request)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    break loop
                }
                
                list, list_ok := store_get(&store, key, or_insert = true)
                
                for request != "" {
                    value, value_ok := chop_line_and_parse_bulk_string(&request)
                    if !value_ok {
                        send_simple_error(client, "ERR", "bad value")
                        break loop
                    }
                    
                    // @speed collect then prepend once to avoid multiple copies
                    value_prepend(list, value)
                }
                
                send_simple_integer(client, len(list.content))
                
            case "LLEN":
                key, key_ok := chop_line_and_parse_bulk_string(&request)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    break loop
                }
                
                list, list_ok := store_get(&store, key)
                send_simple_integer(client, list_ok ? len(list.content) : 0)
                
            case "LRANGE":
                key, key_ok := chop_line_and_parse_bulk_string(&request)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    break loop
                }
                
                start, start_ok := chop_line_and_parse_bulk_string(&request)
                ss, ss_ok := strconv.parse_int(start)
                if !start_ok || !ss_ok {
                    send_simple_error(client, "ERR", "missing start")
                    break loop
                }
                
                end, end_ok := chop_line_and_parse_bulk_string(&request)
                ee, ee_ok := strconv.parse_int(end)
                if !end_ok || !ee_ok {
                    send_simple_error(client, "ERR", "missing end")
                    break loop
                }
                
                list, list_ok := store_get(&store, key)
                if list_ok {
                    count := len(list.content)
                    if ss < 0 {
                        if ss < -count {
                            ss = 0 
                        } else {
                            ss = ((ss % count) + count) % count
                        }
                    }
                    
                    if ee < 0 {
                        ee = ((ee % count) + count) % count
                    } else if ee > count {
                        ee = count-1
                    }
                }
                    
                if ss > ee {
                    list_ok = false
                }
                
                if list_ok && ss >= len(list.content) {
                    list_ok = false
                }
                
                if !list_ok {
                    send_array_nil(client)
                } else {
                    send_array_of_bulk_string(client, list.content[ss:ee+1])
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

store_get :: proc (store: ^Store, key: string, or_insert := false, replace_previous := false) -> (^Value, bool) {
    key := clone_string(key)
    value, value_ok := &store[key]
    
    make_new := false
    if replace_previous {
        if value_ok do free(value)
        make_new = true
    }
    if or_insert && !value_ok {
        make_new = true
    }
    
    if make_new {
        store[key] = {}
        value, value_ok = &store[key]
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
    response := strings.builder_make(context.temp_allocator)
    fmt.sbprintf(&response, "*%v\r\n", len(array))
    for value in array {
        // @copypasta of format
        fmt.sbprintf(&response, "$%v\r\n%v\r\n", len(value), value)
    }
    
    send(client, strings.to_string(response))
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
