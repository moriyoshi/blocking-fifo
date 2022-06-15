// Copyright (c) 2022 Moriyoshi Koizumi
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the “Software”), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package blocking_fifo

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const smallChunkSize = 4096

type readReq struct {
	l       int
	b       []byte
	next    *readReq
	fulfill sync.Cond
}

// BlockingFIFO represents a FIFO buffer that implements both io.Writer and io.Reader interface.
// A BlockingFIFO can have multiple writers and readers, and consumer will block
// if no sufficient data are provided by the writers.
type BlockingFIFO struct {
	blockOnShortage uintptr
	mu              sync.Mutex
	bufs            [][]byte
	l               int
	readClosed      bool
	writeClosed     bool
	readReqs        struct {
		first *readReq
		last  *readReq
	}
}

func (b *BlockingFIFO) tryFulfillReadReq(rr *readReq) bool {
	if b.l >= rr.l {
		fc := &b.bufs[0]
		if len(*fc) >= rr.l {
			if rr.b == nil {
				rr.b = (*fc)[:rr.l]
			} else {
				copy(rr.b, (*fc)[:rr.l])
			}
			nc := (*fc)[rr.l:]
			if len(nc) == 0 {
				b.bufs = b.bufs[1:]
			} else {
				*fc = nc
			}
		} else {
			var ab []byte
			if rr.b == nil {
				ab = make([]byte, 0, rr.l)
			} else {
				ab = rr.b[:0]
			}
			i := 0
			for {
				r := rr.l - len(ab)
				if r == 0 {
					break
				}
				c := &b.bufs[i]
				if len(*c) <= r {
					ab = append(ab, (*c)...)
					i += 1
				} else {
					ab = append(ab, (*c)[:r]...)
					*c = (*c)[r:]
				}
			}
			b.bufs = b.bufs[i:]
			if rr.b == nil {
				rr.b = ab
			}
		}
		b.l -= rr.l
		return true
	} else if b.writeClosed {
		ab := b.drainAll(rr.b)
		if len(ab) > 0 && &rr.b[0] != &ab[0] {
			copy(rr.b, ab)
		}
		rr.l = len(ab)
		return true
	} else if atomic.LoadUintptr(&b.blockOnShortage) == 0 {
		ab := b.drainAll(rr.b)
		if len(ab) == 0 {
			return false
		} else if &rr.b[0] != &ab[0] {
			copy(rr.b, ab)
		}
		rr.l = len(ab)
		return true
	} else {
		return false
	}
}

func (b *BlockingFIFO) drainAll(ab []byte) []byte {
	if ab == nil || cap(ab) < b.l {
		ab = make([]byte, 0, b.l)
	} else {
		ab = ab[:0]
	}
	for i := 0; i < len(b.bufs); i++ {
		ab = append(ab, b.bufs[i]...)
	}
	b.bufs = nil
	b.l = 0
	return ab
}

var errShutdown = fmt.Errorf("already shut down")

// Write writes a chunk of data to the buffer. This function never blocks.
func (b *BlockingFIFO) Write(p []byte) (int, error) {
	b.mu.Lock()
	if b.writeClosed {
		return 0, errShutdown
	}
	if b.readClosed {
		return len(p), nil
	}
	for len(p) > 0 {
		if len(b.bufs) > 0 {
			lc := &b.bufs[len(b.bufs)-1]
			// if the size of the last chunk plus the appended buffer is shorter than the threshold,
			// reuse the chunk.
			if len(*lc)+len(p) < smallChunkSize {
				if cap(*lc) < len(*lc)+len(p) {
					nc := make([]byte, len(*lc), smallChunkSize)
					copy(nc, *lc)
					*lc = nc
				}
				*lc = append(*lc, p...)
				break
			}
		}

		if cap(b.bufs) == 0 {
			b.bufs = make([][]byte, 0, 16)
		}
		b.bufs = append(b.bufs, p)
		break
	}
	b.l += len(p)
	for rr := b.readReqs.first; rr != nil; rr = rr.next {
		rr.fulfill.L.Lock()
		if b.tryFulfillReadReq(rr) {
			b.readReqs.first = rr.next
			if rr.next == nil {
				b.readReqs.last = nil
			}
			if rr.fulfill.L != nil {
				rr.fulfill.Broadcast()
			}
		}
		rr.fulfill.L.Unlock()
	}
	b.mu.Unlock()
	return len(p), nil
}

// CancelAllReads cancels all the pending readers and let them
// return immediately with zero-length read and ErrOperationCanceled.
func (b *BlockingFIFO) CancelAllReads() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for rr := b.readReqs.first; rr != nil; rr = rr.next {
		rr.fulfill.L.Lock()
		rr.l = -1
		rr.fulfill.Broadcast()
		rr.fulfill.L.Unlock()
	}

	b.readReqs.first = nil
	b.readReqs.last = nil
}

var ErrOperationCanceled = fmt.Errorf("operation canceled")

// Read enqueues a read request with the specified amount of data,
// and wait until the request is fulfilled for an indefinite time
// if the FIFO is set to blocking mode. If it is not set so,
// the function returns immediately with the data in the buffer,
// or zero-length read.
func (b *BlockingFIFO) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	if b.readClosed {
		b.mu.Unlock()
		return 0, io.EOF
	}
	if b.writeClosed && b.l == 0 {
		b.mu.Unlock()
		return 0, io.EOF
	}
	rr := &readReq{l: len(p), b: p}
	if b.tryFulfillReadReq(rr) {
		b.mu.Unlock()
		return rr.l, nil
	} else {
		rr.fulfill.L = new(sync.Mutex)
		if b.readReqs.last == nil {
			b.readReqs.first = rr
		} else {
			b.readReqs.last.next = rr
		}
		b.readReqs.last = rr
		b.mu.Unlock()
		rr.fulfill.L.Lock()
		rr.fulfill.Wait()
		rr.fulfill.L.Unlock()
		if rr.l < 0 {
			// canceled
			return 0, ErrOperationCanceled
		} else if rr.l == 0 {
			// zero read must result in io.EOF
			return 0, io.EOF
		} else {
			return rr.l, nil
		}
	}
}

// CloseWrite flags the FIFO so that it no longer accepts
// writes to it. Any further write attempts will fail with
// errors.
func (b *BlockingFIFO) CloseWrite() error {
	b.mu.Lock()
	b.writeClosed = true
	b.mu.Unlock()
	return nil
}

// CloseRead flags the FIFO so that it no longer accepts
// reads from it. The method also instruct it to
// fulfill the pending reads with the remaining data in the
// buffer. Any further read attempts will fail with io.EOF.
func (b *BlockingFIFO) CloseRead() error {
	b.mu.Lock()
	rr := b.readReqs.first
	b.readReqs.first = nil
	b.readReqs.last = nil
	b.readClosed = true
	var bb []byte
	if rr != nil {
		bb = b.drainAll(rr.b)
	} else {
		bb = b.drainAll(nil)
	}
	b.mu.Unlock()

	for ; rr != nil; rr = rr.next {
		rr.fulfill.L.Lock()
		if len(bb) > rr.l {
			if &rr.b[0] == &bb[0] {
				rr.b = bb[:rr.l]
			} else {
				copy(rr.b, bb[:rr.l])
			}
			bb = bb[rr.l:]
		} else {
			rr.l = len(bb)
			if len(bb) > 0 && &rr.b[0] != &bb[0] {
				copy(rr.b, bb)
				bb = nil
			}
		}
		rr.fulfill.Broadcast()
		rr.fulfill.L.Unlock()
	}
	return nil
}

// Close effectively prevents future reads and writes.
func (b *BlockingFIFO) Close() error {
	b.CloseRead()
	b.CloseWrite()
	return nil
}

// Len returns the total size of data in the buffer.
func (b *BlockingFIFO) Len() int {
	b.mu.Lock()
	l := b.l
	b.mu.Unlock()
	return l
}

// SetBlockOnShortage marks the FIFO so that read attempts
// will block when the data in the buffer is insufficient
// to fulfill the read requests.
func (b *BlockingFIFO) SetBlockOnShortage(v bool) {
	var vv uintptr
	if v {
		vv = 1
	}
	atomic.StoreUintptr(&b.blockOnShortage, vv)
}
