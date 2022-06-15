package blocking_fifo

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func gen(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(i)
	}
	return b
}

func TestSimple(t *testing.T) {
	bufSizes := []int{2, 128, 512, 4096, 8192, 131072}
	readSizes := []int{1, 3, 11, 2047, 8191}
	for _, blockOnShortage := range []uintptr{0, 1} {
		for _, bs := range bufSizes {
			wb := gen(bs)
			for _, rs := range readSizes {
				t.Run(fmt.Sprintf("%v %d %d", blockOnShortage, len(wb), rs), func(t *testing.T) {
					b := BlockingFIFO{blockOnShortage: blockOnShortage}
					n, err := b.Write(wb)
					if !assert.NoError(t, err) {
						t.Fail()
					}
					assert.Equal(t, len(wb), n)
					rb := make([]byte, rs)
					for j := 0; j < len(wb); j += rs {
						bytesToRead := len(wb) - j
						if bytesToRead > rs {
							bytesToRead = rs
						}
						n, err := b.Read(rb[:bytesToRead])
						if !assert.NoError(t, err) {
							t.Fail()
						}
						assert.Equal(t, bytesToRead, n)
						assert.Equal(t, wb[j:j+bytesToRead], rb[:bytesToRead])
					}
				})
			}
		}
	}
}

func TestConcurrencyNonblocking(t *testing.T) {
	wb := gen(32767)
	writeSizes := []int{1, 3, 11, 2047, 8191, 16383}
	readSizes := []int{1, 3, 11, 2047, 8191}
	for _, ws := range writeSizes {
		for _, rs := range readSizes {
			t.Run(fmt.Sprintf("%d %d", ws, rs), func(t *testing.T) {
				b := BlockingFIFO{}
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					for j := 0; j < len(wb); j += ws {
						time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond / 1000)
						bytesToWrite := len(wb) - j
						if bytesToWrite > ws {
							bytesToWrite = ws
						}
						n, err := b.Write(wb[j : j+bytesToWrite])
						if !assert.NoError(t, err) {
							t.Fail()
						}
						assert.Equal(t, bytesToWrite, n)
					}
				}()
				go func() {
					defer wg.Done()
					rb := make([]byte, len(wb))
					for j := 0; j < len(wb); {
						time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond / 1000)
						bytesToRead := len(wb) - j
						if bytesToRead > rs {
							bytesToRead = rs
						}
						n, err := b.Read(rb[j : j+bytesToRead])
						if !assert.NoError(t, err) {
							t.Fail()
						}
						j += n
					}
					assert.Equal(t, wb, rb)
				}()
				wg.Wait()
			})
		}
	}
}

func TestConcurrencyBlocking(t *testing.T) {
	wb := gen(65535)
	writeSizes := []int{1, 3, 11, 2047, 8191, 16383}
	readSizes := []int{1, 3, 11, 2047, 8191}
	for _, ws := range writeSizes {
		for _, rs := range readSizes {
			t.Run(fmt.Sprintf("%d %d", ws, rs), func(t *testing.T) {
				b := BlockingFIFO{}
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					for j := 0; j < len(wb); j += ws {
						time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond / 1000)
						bytesToWrite := len(wb) - j
						if bytesToWrite > ws {
							bytesToWrite = ws
						}
						n, err := b.Write(wb[j : j+bytesToWrite])
						if !assert.NoError(t, err) {
							t.Fail()
						}
						assert.Equal(t, bytesToWrite, n)
					}
				}()
				go func() {
					defer wg.Done()
					rb := make([]byte, rs)
					for j := 0; j < len(wb); j += rs {
						time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond / 1000)
						bytesToRead := len(wb) - j
						if bytesToRead > rs {
							bytesToRead = rs
						}
						n, err := b.Read(rb[:bytesToRead])
						if !assert.NoError(t, err) {
							t.Fail()
						}
						assert.Equal(t, bytesToRead, n)
						assert.Equal(t, wb[j:j+bytesToRead], rb[:bytesToRead])
					}
				}()
				wg.Wait()
			})
		}
	}
}

func TestCancelAllReads(t *testing.T) {
	var b BlockingFIFO
	for n := 2; n < 10; n++ {
		var wg, sema sync.WaitGroup
		wg.Add(n)
		sema.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				bb := make([]byte, 2)
				sema.Done()
				_, err := b.Read(bb)
				assert.EqualError(t, err, "operation canceled")
			}()
		}
		sema.Wait()
		b.CancelAllReads()
		wg.Wait()
	}
}

func TestCloseBeforeRead(t *testing.T) {
	for n := 2; n < 10; n++ {
		var wg sync.WaitGroup
		var b BlockingFIFO

		b.Write(gen(11))
		b.CloseWrite()

		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				time.Sleep(time.Duration(i) * time.Millisecond)
				bb := make([]byte, 2)
				l, err := b.Read(bb)
				t.Log(i, l, err)
				c := 11 - i*2
				if c > 2 {
					c = 2
					assert.NoError(t, err)
				} else if c < 0 {
					c = 0
					assert.EqualError(t, err, "EOF")
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, c, l)
			}(i)
		}
		wg.Wait()
	}
}
