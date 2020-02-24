package rin

import (
	"bytes"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func newRing(depth, contexts int) *Ring {
	ring, err := NewRing(&Config{
		QueueDepth:          uint32(depth),
		SubmitQueuePollMode: false,
		BufferSize:          4096,
		UseFixedBuffer:      contexts < 1024,
		MaxContexts:         contexts,
		MaxLoopNoReq:        2,
		MaxLoopNoResp:       2,
	})
	if err != nil {
		panic(err)
	}
	return ring
}

func TestAwaitAndStop(t *testing.T) {
	ring := newRing(32, 4)
	defer ring.Close()

	ctx := ring.NewContext()
	defer ring.Finish(ctx)

	ring.Nop(ctx)
	if _, err := ring.Await(ctx); err != nil {
		t.Fatal(err)
	}

	ring.Nop(ctx)
	if _, err := ring.Await(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReadWritePage(t *testing.T) {
	content := make([]byte, 4096)
	rand.Read(content)

	ring := newRing(32, 4)
	defer ring.Close()

	ctx := ring.NewContext()
	defer ring.Finish(ctx)

	writeAndReadFile(t, ctx, ring, content)
}

func TestSocket(t *testing.T) {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 60336,
	})
	if err != nil {
		t.Fatal(err)
	}

	ring := newRing(2048, 2048)
	defer ring.Close()

	go func() {
		for {
			conn, err := l.AcceptTCP()
			if err != nil {
				return
			}

			go func() {
				fd, err := conn.File()
				if err != nil {
					log.Fatal(err)
					return
				}
				conn.Close()
				defer fd.Close()

				ctx := ring.NewContext()
				defer ring.Finish(ctx)

				for {
					buf := ctx.GetBuffer()
					ring.Read(ctx, int32(fd.Fd()), buf)
					sz, err := ring.Await(ctx)
					if err != nil {
						log.Fatal(err)
						return
					}
					if sz == 0 {
						return
					}

					buf = buf[:sz]
					for i := range buf {
						buf[i]++
					}

					ring.Write(ctx, int32(fd.Fd()), buf)
					_, err = ring.Await(ctx)
					if err != nil {
						log.Fatal(err)
						return
					}
				}
			}()
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 1024; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
				IP:   net.IPv4(127, 0, 0, 1),
				Port: 60336,
			})
			if err != nil {
				log.Fatal(err)
				return
			}
			fd, err := conn.File()
			if err != nil {
				log.Fatal(err)
				return
			}
			defer fd.Close()

			ctx := ring.NewContext()
			defer ring.Finish(ctx)
			rnd := rand.New(rand.NewSource(rand.Int63()))
			for j := 0; j < 10; j++ {
				sz := rnd.Intn(4096) + 1
				buf := ctx.GetBuffer()[:sz]
				rnd.Read(buf)

				ring.Write(ctx, int32(fd.Fd()), buf)
				_, err := ring.Await(ctx)
				if err != nil {
					log.Fatal(err)
					return
				}
				request := append([]byte{}, buf...)

				ring.Read(ctx, int32(fd.Fd()), buf)
				res, err := ring.Await(ctx)
				if err != nil {
					log.Fatal(err)
					return
				}
				if sz != int(res) {
					log.Fatal("short read")
					return
				}
				for i := range buf {
					if buf[i]-request[i] != 1 {
						log.Fatal("incorrect result")
					}
				}

				time.Sleep(5 * time.Second)
			}
		}(i)
	}
	wg.Wait()
	l.Close()
}

func writeAndReadFile(t *testing.T, ctx *Context, ring *Ring, content []byte) {
	f, err := ioutil.TempFile("", "fixture*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	fd, err := unix.Open(f.Name(), unix.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	buf := ctx.GetBuffer()[:len(content)]
	copy(buf, content)

	ring.WriteAt(ctx, int32(fd), buf, 0)
	nn, err := ring.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if int(nn) != len(content) {
		t.Fatal("not write")
	}

	copy(buf, make([]byte, len(content)))
	ring.ReadAt(ctx, int32(fd), buf, 0)
	_, err = ring.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf, content) {
		t.Fatalf("content is incorrect")
	}
	runtime.KeepAlive(f)
}
