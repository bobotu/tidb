// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"encoding/binary"
	"math"
	"unsafe"
)

const (
	alignMask = 1<<32 - 8 // 29 bit 1 and 3 bit 0.

	nullBlockOffset = math.MaxUint32
	maxBlockSize    = 128 << 20
	initBlockSize   = 4 * 1024
)

var (
	nullValueAddr = memdbValueAddr{math.MaxUint32, math.MaxUint32}
	nullNodeAddr  = memdbNodeAddr(math.MaxUint32)
	endian        = binary.LittleEndian
)

type memdbNodeAddr uint32

func encodeNodeAddr(index int, offsetIndex uint32) memdbNodeAddr {
	return memdbNodeAddr((offsetIndex << 8) | uint32(index&0xff))
}

func (addr memdbNodeAddr) index() uint32 {
	return uint32(addr & 0xff)
}

func (addr memdbNodeAddr) offsetIndex() uint32 {
	return uint32(addr >> 8)
}

func (addr memdbNodeAddr) isNull() bool {
	return addr == nullNodeAddr
}

func (addr memdbNodeAddr) store(dst []byte) {
	endian.PutUint32(dst, uint32(addr))
}

func (addr *memdbNodeAddr) load(src []byte) {
	*addr = memdbNodeAddr(endian.Uint32(src))
}

func (addr memdbNodeAddr) size() int {
	return 4
}

type memdbValueAddr struct {
	idx uint32
	off uint32
}

func (addr memdbValueAddr) isNull() bool {
	return addr == nullValueAddr
}

// store and load is used by vlog, due to pointer in vlog is not aligned.

func (addr memdbValueAddr) store(dst []byte) {
	endian.PutUint32(dst, addr.idx)
	endian.PutUint32(dst[4:], addr.off)
}

func (addr *memdbValueAddr) load(src []byte) {
	addr.idx = endian.Uint32(src)
	addr.off = endian.Uint32(src[4:])
}

func (addr memdbValueAddr) size() int {
	return 8
}

type memdbArena struct {
	blockSize int
	blocks    []memdbArenaBlock
}

func (a *memdbArena) enlarge(allocSize, blockSize int) {
	a.blockSize = blockSize
	for a.blockSize <= allocSize {
		a.blockSize <<= 1
	}
	// Size will never larger than maxBlockSize.
	if a.blockSize > maxBlockSize {
		a.blockSize = maxBlockSize
	}
	a.blocks = append(a.blocks, memdbArenaBlock{
		buf:  make([]byte, a.blockSize),
		tail: uint32(a.blockSize),
	})
}

func (a *memdbArena) reset() {
	for i := range a.blocks {
		a.blocks[i].reset()
	}
	a.blocks = a.blocks[:0]
	a.blockSize = 0
}

type memdbArenaBlock struct {
	head uint32
	tail uint32
	buf  []byte
}

func (a *memdbArenaBlock) reset() {
	a.buf = nil
	a.head = 0
	a.tail = 0
}

type memdbCheckpoint struct {
	blockSize     int
	blocks        int
	offsetInBlock uint32
}

func (cp *memdbCheckpoint) isSamePosition(other *memdbCheckpoint) bool {
	return cp.blocks == other.blocks && cp.offsetInBlock == other.offsetInBlock
}

func (a *memdbArena) checkpoint() memdbCheckpoint {
	snap := memdbCheckpoint{
		blockSize: a.blockSize,
		blocks:    len(a.blocks),
	}
	if len(a.blocks) > 0 {
		snap.offsetInBlock = a.blocks[len(a.blocks)-1].head
	}
	return snap
}

func (a *memdbArena) truncate(snap *memdbCheckpoint) {
	for i := snap.blocks; i < len(a.blocks); i++ {
		a.blocks[i] = memdbArenaBlock{}
	}
	a.blocks = a.blocks[:snap.blocks]
	if len(a.blocks) > 0 {
		a.blocks[len(a.blocks)-1].head = snap.offsetInBlock
	}
	a.blockSize = snap.blockSize
}

type nodeAllocator struct {
	memdbArena

	// Dummy node, so that we can make X.left.up = X.
	// We then use this instead of NULL to mean the top or bottom
	// end of the rb tree. It is a black node.
	nullNode memdbNode
}

func (a *nodeAllocator) init() {
	a.nullNode = memdbNode{
		up:    nullNodeAddr,
		left:  nullNodeAddr,
		right: nullNodeAddr,
		vptr:  nullValueAddr,
	}
}

func (a *nodeAllocator) getNode(addr memdbNodeAddr) *memdbNode {
	if addr.isNull() {
		return &a.nullNode
	}

	idx := addr.index()
	offsetIdx := addr.offsetIndex()
	blk := &a.blocks[idx]
	offsetStart := len(blk.buf) - int(offsetIdx+1)*4
	offset := endian.Uint32(blk.buf[offsetStart:])
	return (*memdbNode)(unsafe.Pointer(&blk.buf[offset]))
}

func (a *nodeAllocator) allocNode(key Key) (memdbNodeAddr, *memdbNode) {
	size := 8*4 + 2 + 1 + len(key)
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}

	if len(a.blocks) == 0 {
		a.enlarge(size, initBlockSize)
	}

	addr, mem := a.allocInLastBlock(size)
	if addr.isNull() {
		a.enlarge(size, a.blockSize<<1)
		addr, mem = a.allocInLastBlock(size)
	}

	n := (*memdbNode)(unsafe.Pointer(&mem[0]))
	n.vptr = nullValueAddr
	n.klen = uint16(len(key))
	copy(n.getKey(), key)
	return addr, n
}

func (a *nodeAllocator) allocInLastBlock(size int) (memdbNodeAddr, []byte) {
	idx := len(a.blocks) - 1
	blk := &a.blocks[idx]
	offset := blk.head
	newHead := (offset + uint32(size) + 7) & alignMask
	newTail := blk.tail - 4
	if newHead > newTail {
		return nullNodeAddr, nil
	}

	blk.head, blk.tail = newHead, newTail
	endian.PutUint32(blk.buf[newTail:], offset)
	offsetIdx := (uint32(len(blk.buf))-newTail)/4 - 1
	return encodeNodeAddr(idx, offsetIdx), blk.buf[offset:newHead]
}

var testMode = false

func (a *nodeAllocator) freeNode(addr memdbNodeAddr) {
	if testMode {
		// Make it easier for debug.
		n := a.getNode(addr)
		badAddr := nullValueAddr
		badAddr.idx--
		n.left = memdbNodeAddr(badAddr.idx)
		n.right = memdbNodeAddr(badAddr.idx)
		n.up = memdbNodeAddr(badAddr.idx)
		n.vptr = badAddr
		return
	}
	// TODO: reuse freed nodes.
}

func (a *nodeAllocator) reset() {
	a.memdbArena.reset()
	a.init()
}

type memdbVlogHdr struct {
	valueLen uint32
	nodeAddr memdbNodeAddr
	oldValue memdbValueAddr
}

func (hdr *memdbVlogHdr) store(dst []byte) {
	cursor := 0
	hdr.nodeAddr.store(dst[cursor:])
	cursor += hdr.nodeAddr.size()

	vLen := hdr.valueLen << 1
	if !hdr.oldValue.isNull() {
		hdr.oldValue.store(dst[cursor:])
		cursor += hdr.oldValue.size()
		vLen |= 1
	}

	endian.PutUint32(dst[cursor:], vLen)
}

func (hdr *memdbVlogHdr) load(src []byte) {
	cursor := len(src) - 4
	vLen := endian.Uint32(src[cursor:])
	hdr.valueLen = vLen >> 1

	if vLen&1 != 0 {
		cursor -= hdr.oldValue.size()
		hdr.oldValue.load(src[cursor:])
	} else {
		hdr.oldValue = nullValueAddr
	}

	cursor -= hdr.nodeAddr.size()
	hdr.nodeAddr.load(src[cursor:])
}

func (hdr *memdbVlogHdr) size() int {
	if !hdr.oldValue.isNull() {
		return 16
	}
	return 8
}

type memdbVlog struct {
	memdbArena
}

func (l *memdbVlog) alloc(size int) (memdbValueAddr, []byte) {
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}

	if len(l.blocks) == 0 {
		l.enlarge(size, initBlockSize)
	}

	addr, data := l.allocInLastBlock(size)
	if !addr.isNull() {
		return addr, data
	}

	l.enlarge(size, l.blockSize<<1)
	return l.allocInLastBlock(size)
}

func (l *memdbVlog) allocInLastBlock(size int) (memdbValueAddr, []byte) {
	idx := len(l.blocks) - 1
	blk := &l.blocks[idx]
	offset := int(blk.head)
	newLen := offset + size
	if newLen > len(blk.buf) {
		return nullValueAddr, nil
	}

	blk.head = uint32(newLen)
	addr := memdbValueAddr{uint32(idx), uint32(offset)}
	return addr, blk.buf[offset:newLen]
}

func (l *memdbVlog) appendValue(nodeAddr memdbNodeAddr, oldValue memdbValueAddr, value []byte) memdbValueAddr {
	hdr := memdbVlogHdr{
		nodeAddr: nodeAddr,
		oldValue: oldValue,
		valueLen: uint32(len(value)),
	}

	size := hdr.size() + len(value)
	addr, mem := l.alloc(size)

	copy(mem, value)
	hdr.store(mem[len(value):])

	addr.off += uint32(size)
	return addr
}

func (l *memdbVlog) getValue(addr memdbValueAddr) []byte {
	_, v := l.getHdrAndValue(addr)
	return v
}

func (l *memdbVlog) getHdrAndValue(addr memdbValueAddr) (memdbVlogHdr, []byte) {
	block := l.blocks[addr.idx].buf
	var hdr memdbVlogHdr
	hdr.load(block[:addr.off])
	if hdr.valueLen == 0 {
		return hdr, tombstone
	}
	hdrOff := addr.off - uint32(hdr.size())
	valueOff := hdrOff - hdr.valueLen
	return hdr, block[valueOff:hdrOff:hdrOff]
}

func (l *memdbVlog) getSnapshotValue(addr memdbValueAddr, snap *memdbCheckpoint) ([]byte, bool) {
	result := l.selectValueHistory(addr, func(addr memdbValueAddr) bool {
		return !l.canModify(snap, addr)
	})
	if result.isNull() {
		return nil, false
	}
	return l.getValue(addr), true
}

func (l *memdbVlog) selectValueHistory(addr memdbValueAddr, predicate func(memdbValueAddr) bool) memdbValueAddr {
	for !addr.isNull() {
		if predicate(addr) {
			return addr
		}
		hdr, _ := l.getHdrAndValue(addr)
		addr = hdr.oldValue
	}
	return nullValueAddr
}

func (l *memdbVlog) revertToCheckpoint(db *memdb, cp *memdbCheckpoint) {
	cursor := l.checkpoint()
	for !cp.isSamePosition(&cursor) {
		cursorAddr := memdbValueAddr{idx: uint32(cursor.blocks - 1), off: cursor.offsetInBlock}
		hdr, _ := l.getHdrAndValue(cursorAddr)
		node := db.getNode(hdr.nodeAddr)

		node.vptr = hdr.oldValue
		db.size -= int(hdr.valueLen)
		// oldValue.isNull() == true means this is a newly added value.
		if hdr.oldValue.isNull() {
			// If there are no flags associated with this key, we need to delete this node.
			keptFlags := node.getKeyFlags() & persistentFlags
			if keptFlags == 0 {
				db.deleteNode(node)
			} else {
				node.setKeyFlags(keptFlags)
				db.dirty = true
			}
		} else {
			db.size += len(l.getValue(hdr.oldValue))
		}

		l.moveBackCursor(&cursor, &hdr)
	}
}

func (l *memdbVlog) inspectKVInLog(db *memdb, head, tail *memdbCheckpoint, f func(Key, KeyFlags, []byte)) {
	cursor := *tail
	for !head.isSamePosition(&cursor) {
		cursorAddr := memdbValueAddr{idx: uint32(cursor.blocks - 1), off: cursor.offsetInBlock}
		hdr, value := l.getHdrAndValue(cursorAddr)
		node := db.allocator.getNode(hdr.nodeAddr)

		// Skip older versions.
		if node.vptr == cursorAddr {
			f(node.getKey(), node.getKeyFlags(), value)
		}

		l.moveBackCursor(&cursor, &hdr)
	}
}

func (l *memdbVlog) moveBackCursor(cursor *memdbCheckpoint, hdr *memdbVlogHdr) {
	cursor.offsetInBlock -= uint32(hdr.size()) + hdr.valueLen
	if cursor.offsetInBlock == 0 {
		cursor.blocks--
		if cursor.blocks > 0 {
			cursor.offsetInBlock = l.blocks[cursor.blocks-1].head
		}
	}
}

func (l *memdbVlog) canModify(cp *memdbCheckpoint, addr memdbValueAddr) bool {
	if cp == nil {
		return true
	}
	if int(addr.idx) > cp.blocks-1 {
		return true
	}
	if int(addr.idx) == cp.blocks-1 && addr.off > cp.offsetInBlock {
		return true
	}
	return false
}
