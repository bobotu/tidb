package rin

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
)

type submitQueue struct {
	// fields reference kernel memory.
	khead        *uint32
	ktail        *uint32
	kringMask    *uint32
	kringEntries *uint32
	kflags       *uint32
	kdropped     *uint32
	karray       []uint32
	ksqes        []sqe

	fd         int32
	setupFlags uint32

	head uint32
	tail uint32

	ringMmap []byte
	sqesMmap []byte
}

func newSubmitQueue(fd int32, p *params) (*submitQueue, error) {
	sz := p.sqOff.array + (p.sqEntries * 4)
	ringData, err := mmap(fd, offSqRing, int(sz))
	if err != nil {
		return nil, errors.Trace(err)
	}
	ringPtr := unsafe.Pointer(&ringData[0])

	sqesSz := int(uintptr(p.sqEntries) * unsafe.Sizeof(sqe{}))
	sqesData, err := mmap(fd, offSqes, sqesSz)
	if err != nil {
		unmap(ringData)
		return nil, errors.Trace(err)
	}

	sq := &submitQueue{
		khead:        (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.sqOff.head))),
		ktail:        (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.sqOff.tail))),
		kringMask:    (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.sqOff.ringMask))),
		kringEntries: (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.sqOff.ringEntries))),
		kflags:       (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.sqOff.flags))),
		kdropped:     (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.sqOff.dropped))),
		fd:           fd,
		setupFlags:   p.flags,
		ringMmap:     ringData,
		sqesMmap:     sqesData,
	}
	arrayHdr := (*reflect.SliceHeader)(unsafe.Pointer(&sq.karray))
	arrayHdr.Cap = int(p.sqEntries)
	arrayHdr.Len = int(p.sqEntries)
	arrayHdr.Data = uintptr(ringPtr) + uintptr(p.sqOff.array)

	sqesHdr := (*reflect.SliceHeader)(unsafe.Pointer(&sq.ksqes))
	sqesHdr.Cap = int(p.sqEntries)
	sqesHdr.Len = int(p.sqEntries)
	sqesHdr.Data = uintptr(unsafe.Pointer(&sqesData[0]))

	return sq, nil
}

func (sq *submitQueue) TryPopSqe() *sqe {
	next := sq.tail + 1
	head := sq.head
	if sq.setupFlags&setupSqPoll != 0 {
		head = atomic.LoadUint32(sq.khead)
	}

	if int(next-head) > len(sq.ksqes) {
		return nil
	}

	idx := sq.tail & *sq.kringMask
	sq.tail = next
	return &sq.ksqes[idx]
}

func (sq *submitQueue) Flush(toSubmit, minWait uint32) uint32 {
	flags := enterGetEvents
	if sq.setupFlags&setupSqPoll == 0 {
		// if toSubmit == 0 && minWait == 0 {
		// 	return toSubmit
		// }
		submitted, err := enter(sq.fd, toSubmit, minWait, flags)
		if err != nil {
			panic(fmt.Sprintf("submit request entries failed, this should never occur. err: %s", err))
		}
		if atomic.LoadUint32(sq.kdropped) != 0 {
			panic("submission queue has dropped some request")
		}
		return submitted
	}

	flags |= enterSqWakeup
	if atomic.LoadUint32(sq.kflags)&sqNeedWakeup != 0 {
		// the kernel has signalled to us that the
		// SQPOLL thread that checks the submission
		// queue has terminated due to inactivity,
		// and needs to be restarted.
		if _, err := enter(sq.fd, toSubmit, minWait, flags); err != nil {
			panic(fmt.Sprintf("wakeup kernel poll thread failed, this should never occur. err: %s", err))
		}
	}
	return toSubmit
}

func (sq *submitQueue) PendingCount() uint32 {
	return sq.tail - sq.head
}

func (sq *submitQueue) Submit(toSubmit uint32) {
	mask := *sq.kringMask
	ktail := atomic.LoadUint32(sq.ktail)

	for i := uint32(0); i < toSubmit; i++ {
		idx := ktail & mask
		atomic.StoreUint32(&sq.karray[idx], sq.head&mask)
		ktail++
		sq.head++
	}
	atomic.StoreUint32(sq.ktail, ktail)
}

func (sq *submitQueue) Destroy() {
	unmap(sq.ringMmap)
	unmap(sq.sqesMmap)
	*sq = submitQueue{}
}

type completionQueue struct {
	khead     *uint32
	ktail     *uint32
	kringMask *uint32
	koverflow *uint32
	kcqes     []cqe

	ringMmap []byte
}

func newCompletionQueue(fd int32, p *params) (*completionQueue, error) {
	sz := p.cqOff.cqes + p.cqEntries*uint32(unsafe.Sizeof(cqe{}))
	ringData, err := mmap(fd, offCqRing, int(sz))
	if err != nil {
		return nil, errors.Trace(err)
	}
	ringPtr := unsafe.Pointer(&ringData[0])

	cq := &completionQueue{
		khead:     (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.cqOff.head))),
		ktail:     (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.cqOff.tail))),
		kringMask: (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.cqOff.ringMask))),
		koverflow: (*uint32)(unsafe.Pointer(uintptr(ringPtr) + uintptr(p.cqOff.overflow))),

		ringMmap: ringData,
	}
	cqesHdr := (*reflect.SliceHeader)(unsafe.Pointer(&cq.kcqes))
	cqesHdr.Cap = int(p.cqEntries)
	cqesHdr.Len = int(p.cqEntries)
	cqesHdr.Data = uintptr(ringPtr) + uintptr(p.cqOff.cqes)

	return cq, nil
}

func (cq *completionQueue) Destroy() {
	unmap(cq.ringMmap)
}

type cqIterator struct {
	cq   *completionQueue
	head uint32
	tail uint32
	mask uint32
}

func (cq *completionQueue) NewIterator() *cqIterator {
	return &cqIterator{
		cq:   cq,
		head: atomic.LoadUint32(cq.khead),
		tail: atomic.LoadUint32(cq.ktail),
		mask: *cq.kringMask,
	}
}

func (it *cqIterator) Valid() bool {
	return it.head != it.tail
}

func (it *cqIterator) Next() {
	it.head++
	atomic.AddUint32(it.cq.khead, 1)
}

func (it *cqIterator) Value() *cqe {
	return &it.cq.kcqes[it.head&it.mask]
}
