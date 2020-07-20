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

// +build !windows

package kv

import (
	"runtime"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"golang.org/x/sys/unix"
)

func newMemdbArenaBlock(blockSize int) *memdbArenaBlock {
	if blockSize < maxBlockSize || !EnableOffHeapAlloc {
		return &memdbArenaBlock{
			buf: make([]byte, blockSize),
		}
	}
	buf, err := unix.Mmap(-1, 0, blockSize, unix.PROT_WRITE|unix.PROT_READ, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		panic(errors.Errorf("failed to allocate memory %s", err))
	}
	metrics.OffHeapAllocBytesGauge.Add(float64(blockSize))
	block := &memdbArenaBlock{buf: buf}
	runtime.SetFinalizer(block, (*memdbArenaBlock).reset)
	return block
}

func (a *memdbArenaBlock) reset() {
	size := cap(a.buf)
	if size == maxBlockSize && EnableOffHeapAlloc {
		err := unix.Munmap(a.buf)
		if err != nil {
			panic(errors.Errorf("failed to free memory %s", err))
		}
		metrics.OffHeapAllocBytesGauge.Sub(float64(size))
	}
	a.buf = nil
	a.length = 0
}
