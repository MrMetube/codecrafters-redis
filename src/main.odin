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

handle_client :: proc (task: thread.Task) {
    client := cast(^Client) task.data
    
    Value :: struct {
        content: string,
        expiration: time.Time,
    }
    store: map[string] Value
    
    buffer: [256] u8
    for {
        bytes_read, receive_err := net.recv(client.socket, buffer[:])
        if receive_err != nil {
            fmt.panicf("%s", receive_err)
        }
        
        if bytes_read == 0 {
            send_simple_error(client, "ERR", "Empty request")
            return
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
                line, ok := chop(&request, "\r\n")
                assert(ok)
                content, content_ok := parse_bulk_string(&request, &line)
                assert(content_ok)
                
                send_bulk_string(client, content)
                
            case "SET":
                line, _ = chop(&request, "\r\n")
                key, key_ok := parse_bulk_string(&request, &line)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    return
                }
                
                line, _ = chop(&request, "\r\n")
                value, value_ok := parse_bulk_string(&request, &line)
                if !value_ok {
                    send_simple_error(client, "ERR", "bad value")
                    return
                }
                
                the_value := Value { content = value }
                if request != "" {
                    line, _ = chop(&request, "\r\n")
                    optional, optional_ok := parse_bulk_string(&request, &line)
                    if !optional_ok {
                        send_simple_error(client, "ERR", "bad optional")
                        return
                    }
                    
                    optional = strings.to_upper(optional)
                    switch optional {
                    case "EX":
                        line, _ = chop(&request, "\r\n")
                        seconds, seconds_ok := parse_bulk_string(&request, &line)
                        if !seconds_ok {
                            send_simple_error(client, "ERR", "bad optional")
                            return
                        }
                        
                        secs, secs_ok := strconv.parse_u64(seconds)
                        assert(secs_ok)
                        
                        the_value.expiration = time.time_add(time.now(), time.Second * cast(time.Duration) secs)
                    
                    case "PX":
                        line, _ = chop(&request, "\r\n")
                        milliseconds, milliseconds_ok := parse_bulk_string(&request, &line)
                        if !milliseconds_ok {
                            send_simple_error(client, "ERR", "bad optional")
                            return
                        }
                        
                        millis, millis_ok := strconv.parse_u64(milliseconds)
                        assert(millis_ok)
                        
                        the_value.expiration = time.time_add(time.now(), time.Millisecond * cast(time.Duration) millis)
                        
                    case:
                        send_simple_error(client, "ERR", "unknown optional")
                        return
                    }
                }
                
                store[key] = the_value
                send_simple_string(client, "OK")
                
            case "GET":
                line, ok = chop(&request, "\r\n")
                key, key_ok := parse_bulk_string(&request, &line)
                if !key_ok {
                    send_simple_error(client, "ERR", "missing key")
                    return
                }
                
                value, value_ok := store[key]
                
                if value.expiration != {} {
                    time_left := time.diff(time.now(), value.expiration)
                    if time_left <= 0 {
                        value_ok = false
                    }
                }
                
                if !value_ok {
                    send_bulk_string(client, "")
                } else {
                    send_bulk_string(client, value.content)
                }
                
            case:
                send_simple_error(client, "ERR", "unknown command")
                return
            }
        }
    }
}

send_simple_string :: proc (client: ^Client, data: string) {
    response := fmt.tprintf("+%v\r\n", data)
    net.send(client.socket, transmute([] u8) response)
}

send_simple_error :: proc (client: ^Client, error: string, message: string) {
    response := fmt.tprintf("-%v %v\r\n", error, message)
    net.send(client.socket, transmute([] u8) response)
    net.close(client.socket)
}

send_bulk_string :: proc (client: ^Client, data: string) {
    response := "$-1\r\n"
    if data != "" {
        response = fmt.tprintf("$%v\r\n%v\r\n", len(data), data)
    }
    net.send(client.socket, transmute([] u8) response)
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
