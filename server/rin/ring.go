package rin

import (
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"golang.org/x/sys/unix"
)

type Config struct {
	QueueDepth              uint32
	SubmitQueuePollAffinity uint32
	SubmitQueuePollMode     bool
	UseFixedBuffer          bool
	BufferSize              int

	MaxContexts   int
	MaxLoopNoReq  int
	MaxLoopNoResp int
}

type Ring struct {
	fd    int32
	flags uint32
	conf  *Config

	contexts   contexts
	reqestChan requestChan

	sq *submitQueue
	cq *completionQueue

	inflightQuota   int
	cntNoSubmition  int
	cntNoCompletion int

	eventFd    int32
	eventFdBuf *unix.Iovec

	stopWg     sync.WaitGroup
	stopped    bool
	needWakeup int32
}

func NewRing(conf *Config) (r *Ring, err error) {
	p := new(params)
	if conf.SubmitQueuePollMode {
		p.flags |= setupSqPoll
		p.sqThreadCpu = conf.SubmitQueuePollAffinity
	}

	r = &Ring{conf: conf}

	if err := r.setupEventFd(); err != nil {
		return nil, errors.Trace(err)
	}

	r.fd, err = setup(conf.QueueDepth, p)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if r.fd < 0 {
		panic("invalid ring fd")
	}
	r.flags = p.flags
	r.inflightQuota = int(p.cqEntries)

	err = r.contexts.Init(r.fd, conf)
	if err != nil {
		r.Close()
		return nil, errors.Trace(err)
	}

	r.reqestChan.Init()
	r.sq, err = newSubmitQueue(r.fd, p)
	if err != nil {
		r.Close()
		return nil, errors.Trace(err)
	}

	r.cq, err = newCompletionQueue(r.fd, p)
	if err != nil {
		r.Close()
		return nil, errors.Trace(err)
	}

	go r.loop()

	return r, nil
}

func (r *Ring) setupEventFd() error {
	fd, err := unix.Eventfd(0, unix.EFD_SEMAPHORE)
	if err != nil {
		return errors.Trace(err)
	}
	bufSize := int(unsafe.Sizeof(unix.Iovec{})) + 8
	mem, err := alloc(bufSize)
	if err != nil {
		unix.Close(fd)
		return errors.Trace(err)
	}
	r.eventFd = int32(fd)
	r.eventFdBuf = (*unix.Iovec)(unsafe.Pointer(&mem[8]))
	r.eventFdBuf.Base = &mem[0]
	r.eventFdBuf.Len = 8
	return nil
}

func (r *Ring) setupWakeup() {
	sqe := r.getNextSqe()
	sqe.opcode = opReadv
	sqe.addr = uint64(uintptr(unsafe.Pointer(r.eventFdBuf)))
	sqe.fd = r.eventFd
	sqe.userData = maskWakeUp
	sqe.len = 1
}

func (r *Ring) Close() {
	if r.sq != nil && r.cq != nil {
		r.stopWg.Add(1)
		r.produceRequest(nil, &request{
			op:       opNop,
			userData: maskPoisonPill,
		})
		r.stopWg.Wait()
	}

	if r.sq != nil {
		r.sq.Destroy()
	}
	if r.cq != nil {
		r.cq.Destroy()
	}

	unix.Close(int(r.fd))

	unix.Close(int(r.eventFd))
	var eventFdBuf []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&eventFdBuf))
	hdr.Cap = int(unsafe.Sizeof(unix.Iovec{})) + 8
	hdr.Len = hdr.Cap
	hdr.Data = uintptr(unsafe.Pointer(r.eventFdBuf.Base))
	free(eventFdBuf)
}

func (r *Ring) NewContext() *Context {
	return r.contexts.Acquire()
}

func (r *Ring) Finish(ctx *Context) {
	r.contexts.Release(ctx)
}

func (r *Ring) Await(ctx *Context) (int32, error) {
	ctx.wg.Wait()
	res := ctx.res
	if res < 0 {
		return 0, unix.Errno(-res)
	}
	return res, nil
}

func (r *Ring) Nop(ctx *Context) {
	r.produceRequest(ctx, &request{
		op:       opNop,
		userData: uint64(ctx.id),
	})
}

func (r *Ring) Read(ctx *Context, fd int32, buf []byte) {
	// TODO: A patch has allowed offset be -1 to use offset mantianed by fd,
	// investigate which version of kernel include this patch.
	r.ReadAt(ctx, fd, buf, 0)
}

func (r *Ring) ReadAt(ctx *Context, fd int32, buf []byte, off int) {
	op := opReadv
	if r.conf.UseFixedBuffer {
		op = opReadFixed
	}
	req := &request{
		op:       op,
		userData: uint64(ctx.id),
		fd:       fd,
		off:      uint64(off),
	}
	ctx.setBufferAddrForReq(req, buf)
	r.produceRequest(ctx, req)
}

func (r *Ring) Write(ctx *Context, fd int32, buf []byte) {
	// TODO: A patch has allowed offset be -1 to use offset mantianed by fd,
	// investigate which version of kernel include this patch.
	r.WriteAt(ctx, fd, buf, 0)
}

func (r *Ring) WriteAt(ctx *Context, fd int32, buf []byte, off int) {
	op := opWritev
	if r.conf.UseFixedBuffer {
		op = opWriteFixed
	}
	req := &request{
		op:       op,
		userData: uint64(ctx.id),
		fd:       fd,
		off:      uint64(off),
	}
	ctx.setBufferAddrForReq(req, buf)
	r.produceRequest(ctx, req)
}

func (r *Ring) resetNoSubmissionCnt() {
	r.cntNoSubmition = 0
	if atomic.LoadInt32(&r.needWakeup) == 1 {
		atomic.StoreInt32(&r.needWakeup, 0)
	}
}

func (r *Ring) resetNoCompletionCnt() {
	r.cntNoCompletion = 0
	if atomic.LoadInt32(&r.needWakeup) == 1 {
		atomic.StoreInt32(&r.needWakeup, 0)
	}
}

func (r *Ring) produceRequest(ctx *Context, req *request) {
	if ctx != nil {
		ctx.wg.Add(1)
	}

	r.reqestChan.Send(req)

	// CAS operation contains a cache line write (invalidation) with needed memory fence.
	// To avoid a cache line contention in normal case, use a atomic load before doing CAS.
	if atomic.LoadInt32(&r.needWakeup) == 1 && atomic.CompareAndSwapInt32(&r.needWakeup, 1, 0) {
		var buf [8]byte
		*(*uint64)(unsafe.Pointer(&buf[0])) = 1
		_, err := unix.Write(int(r.eventFd), buf[:])
		if err != nil {
			panic(err)
		}
	}
}

var (
	requestBatchSizeHist = metrics.UringBatchSizeHistogram.WithLabelValues("request")
	submitBatchSizeHist = metrics.UringBatchSizeHistogram.WithLabelValues("submit")
	completionBatchSizeHist = metrics.UringBatchSizeHistogram.WithLabelValues("completion")
	reapReqsTimeHist = metrics.UringProcessTimeHistogram.WithLabelValues("requests")
	reapCompTimeHist = metrics.UringProcessTimeHistogram.WithLabelValues("completion")
)

func (r *Ring) reapRequests() {
	var cnt int
	start := time.Now()
	r.reqestChan.Reap(func(items []request) {
		for _, i := range items {
			// The call of getNextSqe may trap in kernel for a while,
			// this is safe because we don't hold any lock of channel,
			// user can still submit new request to channel.
			// TODO: may be we need to back-pressure channel here.
			cnt++
			e := r.getNextSqe()
			e.userData = i.userData
			e.opcode = i.op
			e.flags = 0
			e.fd = i.fd
			e.addr = i.addr
			e.setBufIndex(i.bufIdx)
			e.offOrAddr2 = i.off
			e.len = i.len
		}
	})
	reapReqsTimeHist.Observe(float64(time.Since(start).Nanoseconds()))
	requestBatchSizeHist.Observe(float64(cnt))
	if cnt == 0 {
		r.cntNoSubmition++
	} else {
		r.resetNoSubmissionCnt()
	}
}

const (
	maskPoisonPill = uint64(0x100000000) << 0
	maskWakeUp     = uint64(0x200000000) << 1
)

func (r *Ring) reapCompletions() {
	var (
		cnt         int
		setupWakeup bool
	)
	start := time.Now()
	for it := r.cq.NewIterator(); it.Valid(); it.Next() {
		cnt++
		e := it.Value()

		if e.userData == maskPoisonPill {
			r.stopped = true
			r.stopWg.Done()
			continue
		}

		if e.userData == maskWakeUp {
			setupWakeup = true
			continue
		}

		ctx := r.contexts.Get(uint32(e.userData))
		ctx.res = e.res
		ctx.wg.Done()
	}
	r.inflightQuota += cnt
	reapCompTimeHist.Observe(float64(time.Since(start).Nanoseconds()))
	completionBatchSizeHist.Observe(float64(cnt))

	if setupWakeup {
		r.setupWakeup()
	}

	if cnt == 0 {
		r.cntNoCompletion++
	} else {
		r.resetNoCompletionCnt()
	}
}

func (r *Ring) getNextSqe() *sqe {
	for {
		e := r.sq.TryPopSqe()
		if e != nil {
			return e
		}

		// Flush some sqes to kernel, so we have some free sqes to use.
		// If the number of inflight requests will cause the cq overflow,
		// we will wait for some request completion, the next loop will
		// reap the completion queue and increase the quota.
		r.submit(true, false)
	}
}

func (r *Ring) submit(waitIfNoQuota, waitIfNeedWakeup bool) int {
	// check quota, for kernel < 5.5 which doesn't have cq back-pressure.
	toSubmit := r.sq.PendingCount()
	if int(toSubmit) > r.inflightQuota {
		r.reapCompletions()
		toSubmit = uint32(r.inflightQuota)
	}
	submitBatchSizeHist.Observe(float64(toSubmit))
	if toSubmit > 0 {
		r.resetNoSubmissionCnt()
	}

	var minWait uint32
	if (waitIfNeedWakeup && atomic.LoadInt32(&r.needWakeup) == 1) || (waitIfNoQuota && toSubmit == 0) {
		minWait = 1
	}

	r.inflightQuota -= int(toSubmit)
	r.sq.Submit(toSubmit)
	if res := r.sq.Flush(toSubmit, minWait); res != toSubmit {
		// TODO: investigate will io_uring_enter do early return,
		// if so we need to adjust userland state. Just print a debug
		// message here.
		println(res, "!=", toSubmit)
	}
	return int(toSubmit)
}

func (r *Ring) loop() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	r.setupWakeup()

	for {
		r.reapRequests()

		// There is no need to wait for quota, because the submission queue may
		// have some space we should put as many as possible requests to the sq.
		r.submit(false, true)

		r.reapCompletions()

		if r.stopped {
			return
		}
		if r.cntNoCompletion >= r.conf.MaxLoopNoResp && r.cntNoSubmition >= r.conf.MaxLoopNoReq {
			atomic.StoreInt32(&r.needWakeup, 1)
		}
	}
}
