package rin

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/pingcap/errors"
	"golang.org/x/sys/unix"
)

type contexts struct {
	sync.Mutex
	ctxs []Context
	next int

	iovecs    []unix.Iovec
	iovecsPtr []byte
	page      []byte
	pagePtr   []byte
}

func (cr *contexts) Init(fd int32, conf *Config) (err error) {
	n := conf.MaxContexts
	fixedBuffer := conf.UseFixedBuffer
	blockSize := conf.BufferSize
	if fixedBuffer && n >= 1024 {
		return errors.New("fixed buffer can only register no more than 1024 buffers")
	}

	cr.iovecsPtr, err = alloc(n * int(unsafe.Sizeof(unix.Iovec{})))
	if err != nil {
		return errors.Trace(err)
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&cr.iovecs))
	hdr.Cap = n
	hdr.Len = n
	hdr.Data = uintptr(unsafe.Pointer(&cr.iovecsPtr[0]))

	cr.page, cr.pagePtr, err = allocPages(n, blockSize)
	if err != nil {
		cr.Destroy()
		return errors.Trace(err)
	}

	cr.ctxs = make([]Context, n)
	for i := range cr.ctxs {
		ctx := &cr.ctxs[i]
		ctx.id = uint32(i + 1)
		buf := cr.page[i*blockSize : (i+1)*blockSize]
		ctx.buf = buf
		ctx.bufIdx = i
		ctx.bufVec = &cr.iovecs[i]
		ctx.bufVec.Len = uint64(blockSize)
		ctx.bufVec.Base = &buf[0]
	}

	if fixedBuffer {
		// We can only register no more than 1024 buffers (limited by UIO_MAXIOV)
		if err := register(fd, registerBuffers, unsafe.Pointer(&cr.iovecs[0]), n); err != nil {
			cr.Destroy()
			return errors.Trace(err)
		}
	}

	return nil
}

func (cr *contexts) Destroy() {
	if cr.iovecsPtr != nil {
		free(cr.iovecsPtr)
		cr.iovecs = nil
	}
	if cr.pagePtr != nil {
		free(cr.pagePtr)
		cr.pagePtr = nil
	}
}

func (cr *contexts) Acquire() *Context {
	cr.Lock()
	defer cr.Unlock()
	if cr.next >= len(cr.ctxs) {
		return nil
	}

	ctx := &cr.ctxs[cr.next]
	ctx.id, cr.next = uint32(cr.next), int(ctx.id)
	return ctx
}

func (cr *contexts) Release(ctx *Context) {
	cr.Lock()
	defer cr.Unlock()
	ctx.id, cr.next = uint32(cr.next), int(ctx.id)
}

func (cr *contexts) Get(id uint32) *Context {
	return &cr.ctxs[id]
}

type Context struct {
	wg  sync.WaitGroup
	res int32

	// used as next free index in free-list
	id     uint32
	bufIdx int
	bufVec *unix.Iovec
	buf    []byte
}

func (ctx *Context) GetBuffer() []byte {
	return ctx.buf
}

func (ctx *Context) setBufferAddrForReq(req *request, buf []byte) {
	if req.op == opReadFixed || req.op == opWriteFixed {
		req.addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
		req.len = uint32(len(buf))
		req.bufIdx = uint16(ctx.bufIdx)
		return
	}

	ctx.bufVec.Base = &buf[0]
	ctx.bufVec.Len = uint64(len(buf))
	req.addr = uint64(uintptr(unsafe.Pointer(ctx.bufVec)))
	req.len = 1
}
