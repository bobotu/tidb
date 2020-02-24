package rin

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

type requestChan struct {
	slots []chanSlot
}

func (c *requestChan) Init() {
	c.slots = make([]chanSlot, runtime.GOMAXPROCS(0))
}

func (c *requestChan) Send(i *request) {
	pid := procPin()
	c.slots[pid].push(*i)
	procUnpin()
}

func (c *requestChan) Reap(reaper func(items []request)) {
	for i := range c.slots {
		reaper(c.slots[i].reap())
	}
}

type request struct {
	op       uint8
	bufIdx   uint16
	fd       int32
	userData uint64
	addr     uint64
	off      uint64
	len      uint32
}

type chanSlot struct {
	chanSlotInner
	// Avoid false sharing between different slot.
	pad [128 - slotSize%128]byte
}

const slotSize = unsafe.Sizeof(chanSlotInner{})

type chanSlotInner struct {
	// We cannot use sync.Mutex with runtime.procPin, if so the scheduler will paniced.
	// Because the critical region is small and nearly no contention, use a simple spin lock instead.
	lock     int32
	writable []request
	readonly []request
}

func (s *chanSlot) reap() []request {
	s.Lock()
	s.writable, s.readonly = s.readonly[:0], s.writable
	s.Unlock()
	return s.readonly
}

func (s *chanSlot) Lock() {
	for {
		if atomic.CompareAndSwapInt32(&s.lock, 0, 1) {
			return
		}
	}
}

func (s *chanSlot) Unlock() {
	atomic.StoreInt32(&s.lock, 0)
}

func (s *chanSlot) push(i request) {
	s.Lock()
	s.writable = append(s.writable, i)
	s.Unlock()
}

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin() int
