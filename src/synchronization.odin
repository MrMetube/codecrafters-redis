#+vet !unused-procedures
#+no-instrumentation
package main

import "base:intrinsics"
import "core:fmt"

////////////////////////////////////////////////
// Acquire:    Nothing after this, can be done until this is completed.
// Release:    Everything before this point has to be done.
// Full Fence: Acquire and Release
////////////////////////////////////////////////
// Atomics

atomic_compare_exchange :: proc (destination: ^$T, old_value, new_value: T) -> (ok: bool, was_value: T) {
    was_value, ok = intrinsics.atomic_compare_exchange_strong(destination, old_value, new_value)
    return ok, was_value
}

volatile_load      :: intrinsics.volatile_load
volatile_store     :: intrinsics.volatile_store
atomic_add         :: intrinsics.atomic_add
atomic_exchange    :: intrinsics.atomic_exchange

read_cycle_counter :: intrinsics.read_cycle_counter

complete_previous_writes_before_future_writes :: proc () {
    intrinsics.atomic_thread_fence(.Release)
}
@(enable_target_feature="sse2")
complete_previous_reads_before_future_reads :: proc () {
    intrinsics.atomic_thread_fence(.Acquire)
}

// @study(viktor): When would "Sequentially Consistent" ordering be required. Reminder: It ensures not just local order, but global order between all threads and all other "Sequentially Consistent" fences(if I understood it correctly).
compiler_fence :: proc () {
    intrinsics.atomic_signal_fence(.Acq_Rel)
}
compiler_and_cpu_fence :: proc () {
    intrinsics.atomic_thread_fence(.Acq_Rel)
}

////////////////////////////////////////////////

TicketMutex :: struct #align(64) {
    ticket:  u64,
    serving: u64,
}

begin_ticket_mutex :: proc (mutex: ^TicketMutex) {
    ticket := ticket_mutex_take_ticket(mutex)
    for !ticket_mutex_ticket_is_ready(mutex, ticket) {
        spin_hint()
    }
}

ticket_mutex_take_ticket :: proc (mutex: ^TicketMutex) -> u64 {
    result := atomic_add(&mutex.ticket, 1)
    return result
}

ticket_mutex_ticket_is_ready :: proc (mutex: ^TicketMutex, ticket: u64) -> bool {
    result := ticket == volatile_load(&mutex.serving)
    return result
}

end_ticket_mutex :: proc (mutex: ^TicketMutex) {
    atomic_add(&mutex.serving, 1)
}

spin_hint :: proc () {
    // @study(viktor): what does intrinsics.cpu_relax compile to?, is this similar?
    when ODIN_ARCH == .amd64 || ODIN_ARCH == .i386 {
        asm { "pause" , "" } ()
    } else when ODIN_ARCH == .arm32 || ODIN_ARCH == .arm64 {
        asm { "yield", "" } ()
    } else when ODIN_ARCH == .riscv64 || ODIN_ARCH == .wasm32 || ODIN_ARCH == .wasm64p32 || ODIN_ARCH == .Unknown {
        // no-op fallback
    } else {
        #assert(false, "new microarch was added")
    }
}