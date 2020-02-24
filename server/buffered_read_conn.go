// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"net"
	"os"

	"github.com/pingcap/tidb/server/rin"
)

const defaultReaderSize = 16 * 1024

// bufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
type bufferedReadConn struct {
	net.Conn
	ctx  *rin.Context
	ring *rin.Ring
	rb   *rin.ConnReader
	fd   *os.File
}

func (conn bufferedReadConn) Read(b []byte) (n int, err error) {
	return conn.rb.Read(b)
}

func newBufferedReadConn(ring *rin.Ring, conn net.Conn) *bufferedReadConn {
	ctx := ring.NewContext()
	fd, err := conn.(*net.TCPConn).File()
	if err != nil {
		panic(err)
	}
	return &bufferedReadConn{
		Conn: conn,
		rb:   ring.NewReader(ctx, fd),
		ctx:  ctx,
		ring: ring,
		fd:   fd,
	}
}

func (conn bufferedReadConn) Close() error {
	conn.ring.Finish(conn.ctx)
	conn.fd.Close()
	return conn.Conn.Close()
}
