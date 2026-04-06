package main

import "core:sync"
import "core:time"

list_append :: proc (list: ^Value, s: string, loc := #caller_location) {
    assert(list.kind == .List, loc = loc)
    
    s := clone_string(s, context.allocator)
    
    append(&list.content, s)
    sync.sema_post(&list.content_semaphore, 1)
}

list_prepend :: proc (list: ^Value, s: string, loc := #caller_location) {
    assert(list.kind == .List, loc = loc)
    
    s := clone_string(s, context.allocator)
    
    inject_at(&list.content, 0, s)
    sync.sema_post(&list.content_semaphore, 1)
}

list_pop :: proc (list: ^Value, loc := #caller_location) -> string {
    assert(list.kind == .List, loc = loc)
    
    result := pop_front(&list.content, loc = loc)
    return result
}

list_len :: proc (list: ^Value, loc := #caller_location) -> int {
    result: int
    
    if list != nil {
        assert(list.kind == .List, loc = loc)
        result = len(list.content)
    }
    
    return result
}

list_slice :: proc (list: ^Value, start, stop: int, loc := #caller_location) -> [] string {
    assert(list.kind == .List, loc = loc)
    
    start, stop := start, stop
    
    list_ok := list != nil
    if list_ok {
        count := len(list.content)
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
    
    if list_ok && start >= len(list.content) {
        list_ok = false
    }
    
    result: [] string
    if list_ok {
        result = list.content[start:stop+1]
    }
    return result
}

list_block :: proc (list: ^Value, timeout := max(time.Duration), loc := #caller_location) -> bool {
    assert(list.kind == .List, loc = loc)
    
    ok := sync.sema_wait_with_timeout(&list.content_semaphore, timeout)
    return ok
}