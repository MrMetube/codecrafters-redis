package main

import "core:fmt"
import "core:net"
import "core:strings"
import "core:thread"
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
    
    buffer: [256] u8
    for {
        bytes_read, receive_err := net.recv(client.socket, buffer[:])
        if receive_err != nil {
            fmt.panicf("%s", receive_err)
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
            fmt.eprintf("remaining `%v`\n", request)
            
            command, command_ok := parse_bulk_string(&request, &line)
            assert(command_ok)
            fmt.eprintf("remaining `%v`\n", request)
            
            // @todo(viktor): case insensitive
            switch command {
            case "PING":
                send_simple_string(client, "PONG")
                
            case "ECHO":
                line, ok := chop(&request, "\r\n")
                assert(ok)
                content, content_ok := parse_bulk_string(&request, &line)
                assert(content_ok)
                
                send_bulk_string(client, content)
                
            case:
                // @todo(viktor): bad request
                net.close(client.socket)
            }
        }
    }
}

send_simple_string :: proc (client: ^Client, data: string) {
    response := fmt.tprintf("+%v\r\n", data)
    net.send(client.socket, transmute([] u8) response)
}

send_bulk_string :: proc (client: ^Client, data: string) {
    response := fmt.tprintf("$%v\r\n%v\r\n", len(data), data)
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
