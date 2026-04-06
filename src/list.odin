package main

import "core:sync"
import "core:time"

list_append :: proc (list: ^Value, value: string, loc := #caller_location) {
    assert(list.kind == .List, loc = loc)
    value_add_item(list, value, len(list.items))
}

list_prepend :: proc (list: ^Value, value: string, loc := #caller_location) {
    assert(list.kind == .List, loc = loc)
    value_add_item(list, value, 0)
}

list_pop :: proc (list: ^Value, loc := #caller_location) -> string {
    assert(list.kind == .List, loc = loc)
    
    result := pop_front(&list.items, loc = loc)
    return result
}

list_len :: proc (list: ^Value, loc := #caller_location) -> int {
    result: int
    
    if list != nil {
        assert(list.kind == .List, loc = loc)
        result = len(list.items)
    }
    
    return result
}

list_slice :: proc (list: ^Value, start, stop: int, loc := #caller_location) -> [] string {
    assert(list.kind == .List, loc = loc)
    
    start, stop := start, stop
    
    list_ok := list != nil
    if list_ok {
        count := len(list.items)
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
    
    if list_ok && start >= len(list.items) {
        list_ok = false
    }
    
    result: [] string
    if list_ok {
        result = list.items[start:stop+1]
    }
    return result
}

list_block :: proc (list: ^Value, timeout := max(time.Duration), loc := #caller_location) -> bool {
    assert(list.kind == .List, loc = loc)
    
    ok := sync.sema_wait_with_timeout(&list.items_semaphore, timeout)
    return ok
}