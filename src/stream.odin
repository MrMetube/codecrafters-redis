package main

Stream_Entry :: struct {
    id: Stream_Id,
    kv_start: int,
    kv_count: int,
}

Stream_Id :: struct {
    millis:   int,
    sequence: int,
}

Stream_Key_Value :: struct {
    key:   string,
    value: string,
}

Id_Error :: enum {
    none,
    id_too_smol,
    bad_nil_value,
}

////////////////////////////////////////////////

// @todo(viktor): should the stream be locked by a mutex for the whole operation?
stream_begin_entry :: proc (stream: ^Value, id_string: string, loc := #caller_location) -> (^Stream_Entry, Id_Error) {
    assert(stream.kind == .Stream, loc = loc)
    
    id, id_ok := parse_id(id_string) 
    
    error := Id_Error.none
    if len(stream.entries) > 0 {
        last := stream.entries[len(stream.entries)-1]
        
        if id.millis < last.id.millis {
            error = .id_too_smol
        }
        
        if id.millis == last.id.millis {
            if id.sequence == -1 {
                id.sequence = last.id.sequence + 1
            } else {
                if id.sequence <= last.id.sequence {
                    error = .id_too_smol
                }
            }
        }
    }
    
    if id.sequence == -1 {
        if id.millis == 0 {
            id.sequence = 1
        } else {
            id.sequence = 0
        }
    }
    
    assert(id.millis != -1)
    assert(id.sequence != -1)
    
    if id.millis == 0 && id.sequence == 0 {
        error = .bad_nil_value
    }
    
    result: ^Stream_Entry
    if error == .none {
        count := len(stream.entries)
        append_nothing(&stream.entries)
        result = &stream.entries[count]
        result.id = id
        result.kv_start = len(stream.entries_kv)
    }
    
    return result, error
}

stream_add_item :: proc (stream: ^Value, entry: ^Stream_Entry, key, value: string, loc := #caller_location) {
    assert(stream.kind == .Stream, loc = loc)
    
    key   := clone_string(key, context.allocator)
    value := clone_string(value, context.allocator)
    append(&stream.entries_kv, Stream_Key_Value{key, value})
    entry.kv_count += 1
}

stream_end_entry :: proc (stream: ^Value, entry: ^Stream_Entry, loc := #caller_location) {
    assert(stream.kind == .Stream, loc = loc)
}
