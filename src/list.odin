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

////////////////////////////////////////////////

list_len :: proc (list: Value, loc := #caller_location) -> int {
    assert(list.kind == .List, loc = loc)
    result := len(list.items)
    
    return result
}

list_slice :: proc (list: Value, start, stop: int, loc := #caller_location) -> [] string {
    assert(list.kind == .List, loc = loc)
    
    result := value_slice(list, start, stop)
    return result
}

////////////////////////////////////////////////

list_block :: proc (list: ^Value, timeout := max(time.Duration), loc := #caller_location) -> bool {
    assert(list.kind == .List, loc = loc)
    
    ok := sync.sema_wait_with_timeout(&list.items_semaphore, timeout)
    return ok
}