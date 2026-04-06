package main

zset_add :: proc (set: ^Value, score: f64, value: string, loc := #caller_location) -> int {
    assert(set.kind == .ZSet, loc = loc)
    
    // @speed hash lookup
    is_update := false
    for it, it_index in set.items {
        if it == value {
            it_score := set.members_score[it_index]
            is_update = true
            // @copypasta
            ordered_remove(&set.items,         it_index)
            ordered_remove(&set.members_score, it_index)
            break
        }
    }
    
    index := len(set.items)
    search: for it_score, score_index in set.members_score {
        if it_score == score {
            it_content := set.items[score_index]
            if value < it_content {
                index = score_index
            } else {
                index = score_index+1
            }
            break search
        }
        
        if it_score > score {
            index = score_index
            break search
        }
    }
    
    value_add_item(set, value, index)
    inject_at(&set.members_score, index, score)
    
    result: int
    if !is_update {
        result += 1
    }
    
    return result
}

zset_remove :: proc (set: ^Value, value: string, loc := #caller_location) -> int {
    assert(set.kind == .ZSet, loc = loc)
    
    index, ok := zset_rank(set^, value, loc)
    result: int
    if ok {
        ordered_remove(&set.items,         index)
        ordered_remove(&set.members_score, index)
        result += 1
    }
    
    return result
}

////////////////////////////////////////////////

zset_rank :: proc (set: Value, value: string, loc := #caller_location) -> (int, bool) {
    assert(set.kind == .ZSet, loc = loc)
    
    result: int
    ok: bool
    
    search: for it, it_index in set.items {
        if it == value {
            result = it_index
            ok = true
            break search
        }
    }
    
    return result, ok
}

zset_score :: proc (set: Value, value: string, loc := #caller_location) -> (f64, bool) {
    assert(set.kind == .ZSet, loc = loc)
    
    index, ok := zset_rank(set, value, loc)
    result := set.members_score[index]
    
    return result, ok
}

zset_card :: proc (set: Value, loc := #caller_location) -> int {
    assert(set.kind == .ZSet, loc = loc)
    
    result := len(set.items)
    return result
}

zset_slice :: proc (set: Value, start, stop: int, loc := #caller_location) -> [] string {
    assert(set.kind == .ZSet, loc = loc)
    
    result := value_slice(set, start, stop)
    return result
}
