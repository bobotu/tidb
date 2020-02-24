package rin

import (
	"log"
	"math/bits"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	trapSetup    = unix.SYS_IO_URING_SETUP
	trapEnter    = unix.SYS_IO_URING_ENTER
	trapRegister = unix.SYS_IO_URING_REGISTER
)

type cqe struct {
	userData uint64
	res      int32
	flags    uint32
}

// IO submission data structure (Submission Queue Entry)
type sqe struct {
	// type of operation for this sqe
	opcode uint8
	// seq flags
	flags uint8
	// ioprio for the request
	ioprio uint16
	// file descriptor to do IO on
	fd int32

	// union {
	//	__u64	off;	/* offset into file */
	//	__u64	addr2;
	// }
	offOrAddr2 uint64
	// pointer to buffer or iovecs
	addr uint64
	// uffer size or number of iovecs
	len uint32

	// union {
	//	__kernel_rwf_t	rw_flags;
	//	__u32		fsync_flags;
	//	__u16		poll_events;
	//	__u32		sync_range_flags;
	//	__u32		msg_flags;
	//	__u32		timeout_flags;
	//	__u32		accept_flags;
	//	__u32		cancel_flags;
	// }
	reqFlags uint32
	// data to be passed back at completion time
	userData uint64

	// union {
	//	__u16	buf_index;	/* index into fixed buffers, if used */
	//	__u64	__pad2[3];
	// };
	bufIndex [3]uint64
}

func (e *sqe) setReqFlagsU16(f uint16) {
	*(*uint16)(unsafe.Pointer(&e.reqFlags)) = f
}

func (e *sqe) setReqFlagsU32(f uint32) {
	e.reqFlags = f
}

func (e *sqe) setBufIndex(idx uint16) {
	*(*uint16)(unsafe.Pointer(&e.bufIndex[0])) = idx
}

// sqe->flags
const (
	sqeFixedFile = 1 << iota
	sqeIoDrain
	sqeIoLink
	sqeIoHardlink
)

// io_uring_setup() flags
const (
	setupIoPoll = 1 << iota
	setupSqPoll
	setupSqAff
	setupCqSize
)

const (
	opNop uint8 = iota
	opReadv
	opWritev
	opFsync
	opReadFixed
	opWriteFixed
	opPollAdd
	opPollRemove
	opSyncFileRange
	opSendMsg
	opRecvMsg
	opTimeout
	opTimeoutRemove
	opAccept
	opAsyncCancel
	opLinkTimeout
	opConnect

	opLast
)

// seq->fsync_flags
const (
	fsyncDataSync = 1 << iota
)

// seq->timeout_flags
const (
	timeoutAbs = 1 << iota
)

// IO completion data structure (Completion Queue Entry)
type ceq struct {
	// sqe.userData submission passed back
	userData uint64
	// result code for this event
	res   int32
	flags uint32
}

// Magic offsets for the application to mmap the data it needs
const (
	offSqRing = int64(0)
	offCqRing = int64(0x8000000)
	offSqes   = int64(0x10000000)
)

// Filled with the offset for mmap(2)
type sqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

const (
	sqNeedWakeup = 1 << iota
)

type cqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	resv        [2]uint64
}

// io_uring_enter(2) flags
const (
	enterGetEvents uint32 = 1 << iota
	enterSqWakeup
)

type params struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCpu  uint32
	sqThreadIdle uint32
	features     uint32
	resv         [4]uint32
	sqOff        sqRingOffsets
	cqOff        cqRingOffsets
}

// params.features flags
const (
	featureSingleMmap = 1 << iota
	featureNoDrop
	featureSubmitStable
)

const (
	registerBuffers = iota
	unregisterBuffers
	registerFiles
	unregisterFiles
	registerEventFd
	unregisterEventFd
	registerFilesUpdate
	registerEventFdAsync
)

func alloc(size int) ([]byte, error) {
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_NORESERVE)
}

func free(b []byte) error {
	return unmap(b)
}

const pageSize = 4096

func alignment(block []byte) int {
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(pageSize-1))
}

func allocPages(n, blockSize int) ([]byte, []byte, error) {
	size := n * blockSize
	block, err := alloc(size + pageSize)
	if err != nil {
		return nil, nil, err
	}
	a := alignment(block)
	offset := 0
	if a != 0 {
		offset = pageSize - a
	}
	aligned := block[offset : offset+size]
	// Can't check alignment of a zero sized block
	if size != 0 {
		a = alignment(aligned)
		if a != 0 {
			log.Fatal("Failed to align block")
		}
	}
	return aligned, block, nil
}

func mmap(fd int32, offset int64, size int) ([]byte, error) {
	m, err := unix.Mmap(int(fd), offset, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func unmap(b []byte) error {
	return unix.Munmap(b)
}

func setup(entries uint32, p *params) (int32, error) {
	if entries < 1 || entries > 4096 {
		panic("entries must be between 1 and 4096")
	}
	if bits.OnesCount32(entries) != 1 {
		panic("entries must be a power of 2")
	}

	ret, _, err := unix.Syscall(trapSetup, uintptr(entries), uintptr(unsafe.Pointer(p)), uintptr(0))
	if int(ret) < 0 {
		return 0, err
	}
	return int32(ret), nil
}

func enter(fd int32, toSubmit, minComplete, flags uint32) (uint32, error) {
	for {
		ret, _, err := unix.Syscall6(trapEnter, uintptr(fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), uintptr(0), uintptr(unsafe.Sizeof(unix.Sigset_t{})))
		if int(ret) < 0 {
			if err == unix.EINTR {
				continue
			}
			return 0, err
		}
		return uint32(ret), nil
	}
}

func register(fd int32, opcode int, arg unsafe.Pointer, argLen int) error {
	ret, _, err := unix.Syscall6(trapRegister, uintptr(fd), uintptr(opcode), uintptr(arg), uintptr(argLen), 0, 0)
	if int(ret) < 0 {
		return err
	}
	return nil
}
