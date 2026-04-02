package main

import "core:fmt"
import "core:net"
import "core:strings"
import "core:strconv"
import "core:bytes"

main :: proc (){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    fmt.eprintln("Logs from your program will appear here!")

    // Uncomment the code below to pass the first stage
    listen_socket, listen_err := net.listen_tcp(net.Endpoint{
        port = 6379,
        address = net.IP4_Loopback
    })
    if listen_err != nil {
        fmt.panicf("%s", listen_err)
    }
    client_socket, client_endpoint, accept_err := net.accept_tcp(listen_socket)
    if accept_err != nil {
        fmt.panicf("%s", accept_err)
    }
    
    buffer: [256] u8
    bytes_read, receive_err := net.recv(client_socket, buffer[:])
    if receive_err != nil {
        fmt.panicf("%s", receive_err)
    }
    
    text := transmute(string) buffer[:bytes_read]
    count: u64
    if text[0] == '*' {
        text = text[1:]
        line := chop(&text, "\r\n")
        ok: bool
        count, ok = strconv.parse_u64(line)
        assert(ok)
    }
    
    for i in 0..<count {
        length: u64
        if text[0] == '$' {
            text = text[1:]
            line := chop(&text, "\r\n")
            ok: bool
            length, ok = strconv.parse_u64(line)
            assert(ok)
        }
        
        
        message := text[:length]
        if message == "PING" {
            response := "+PONG\r\n"
            net.send(client_socket, transmute([] u8) response)
        } else {
            // @todo(viktor): bad request
            net.close(client_socket)
        }
    }
}


chop :: proc (s: ^string, until: string) -> string {
    head, match, tail := strings.partition(s^, until)
    s^ = tail
    return head
}
