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
        
        text := transmute(string) buffer[:bytes_read]
        line := chop(&text, "\r\n")
        count: u64
        if line[0] == '*' {
            line = line[1:]
            ok: bool
            count, ok = strconv.parse_u64(line)
            assert(ok)
        }
        
        for i in 0..<count {
            line := chop(&text, "\r\n")
            fmt.eprintf("remaining `%v`\n", text)
            length: u64
            if line[0] == '$' {
                line = line[1:]
                ok: bool
                length, ok = strconv.parse_u64(line)
                assert(ok)
            }
            
            message := chop(&text, "\r\n")
            fmt.eprintf("remaining `%v`\n", text)
            message  = message[:length]
            if message == "PING" {
                response := "+PONG\r\n"
                net.send(client.socket, transmute([] u8) response)
            } else {
                // @todo(viktor): bad request
                net.close(client.socket)
            }
        }
    }
}

chop :: proc (s: ^string, until: string) -> string {
    head, match, tail := strings.partition(s^, until)
    s^ = tail
    return head
}
