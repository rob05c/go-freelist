package freelist

import (
	"errors"
	"io"
	"sync"
	"time"
)

// FreeList implements a free list.
// This is designed to reduce GC pressure in an application.
// All bytes are pre-allocated on New and never deleted until the FreeList itself is garbage-collected.
//
// When a
type FreeList interface {
	Size() int
	BlockSize() int
	NewBuffer() Buffer
}

type freeList struct {
	bts               []byte
	blockSize         int
	unallocatedBlocks []int
	m                 sync.Mutex
	returnBuffer      func() (Buffer, error)
}

//New creates and returns a new FreeList of the given sizeBytes, with the given blockSize.
// If sizeBytes is not a multiple of blockSize, it is rounded up to the next blockSize bytes.
//
// returnBuffer is called when a new block is needed, and all blocks have been used by Buffers (this will be frequent, as soon as all blocks are used, they'll be constantly full and need returning).
//
// The returnBuffer func must be safe for calling by multiple goroutines.
// The returned Buffer must have a ReadWriterCount of 0, and no functions--particularly newReader or Write--may be called on a Buffer after it is returned by returnBuffer.
// Therefore, the owner of the Buffer and returnBuffer must synchronize calls to returnBuffer, Write, and NewReader.
//
// This synchronization will typically be done by the structure caching Buffers, by actually synchronizing giving out Buffers with returnBuffer (rather than synchronizing Write and NewReader themselves).
//
// If returnBuffer returns an error, the Write which triggered it will also return an error.
//
func New(sizeBytes int, blockSizeBytes int, returnBuffer func() (Buffer, error)) FreeList {
	realSizeBytes := sizeBytes
	if realSizeBytes%blockSizeBytes != 0 {
		realSizeBytes = sizeBytes / blockSizeBytes * blockSizeBytes
		if realSizeBytes < sizeBytes {
			realSizeBytes += blockSizeBytes
		}
	}

	numBlocks := realSizeBytes / blockSizeBytes
	unallocatedBlocks := make([]int, numBlocks)
	for i := 0; i < len(unallocatedBlocks); i++ {
		unallocatedBlocks[i] = i
	}

	return &freeList{
		bts:               make([]byte, realSizeBytes),
		blockSize:         blockSizeBytes,
		unallocatedBlocks: unallocatedBlocks,
		returnBuffer:      returnBuffer,
	}
}

var ErrBadReturnBuffer = errors.New("freelist: returnBuffer returned Buffer not created by this FreeList")

func (fl *freeList) getNewBlock() (int, error) {
	fl.m.Lock()
	defer fl.m.Unlock() // TODO reduce lock scope
	blockI := -1

	// If there exist unallocated blocks, grab one and return
	if len(fl.unallocatedBlocks) != 0 {
		blockI = fl.unallocatedBlocks[len(fl.unallocatedBlocks)-1]
		fl.unallocatedBlocks = fl.unallocatedBlocks[:len(fl.unallocatedBlocks)-1]
		return blockI, nil
	}

	// If there are no unallocated blocks, get one with returnBuffer
	for {
		bufI, err := fl.returnBuffer()
		if err != nil { // TODO log original error?
			bufI, err = fl.returnBuffer() // if we got an error, try again once.
		}
		if err != nil {
			return -1, err
		}

		buf, ok := bufI.(*buffer)
		if !ok {
			return -1, ErrBadReturnBuffer
		}

		blocks := buf.blocks
		if len(blocks) == 0 {
			continue // if we got a returned buffer with no blocks, try again
		}
		// take the first one
		blockI = blocks[len(blocks)]
		// free the rest
		blocks = blocks[:len(blocks)-1]
		for _, block := range blocks {
			fl.unallocatedBlocks = append(fl.unallocatedBlocks, block)
		}
		break
	}
	return blockI, nil
}

func (fl *freeList) getBlockBytes(blockIdx int) []byte {
	return fl.bts[blockIdx*fl.blockSize : blockIdx*fl.blockSize+fl.blockSize]
}

func (fl *freeList) Size() int      { return len(fl.bts) }
func (fl *freeList) BlockSize() int { return fl.blockSize }

// Buffer is used to get FreeList bytes.
// Readers may read concurrently with writes. Each concurrent readers will read from the start, and block after the end, until Close is called.
// The Buffer must call Close when it is done writing. Otherwise, readers will block forever.
// Writer implements io.WriteCloser.
//
// Write is not safe for multiple goroutines. There may only ever be one goroutine calling Write.
//
type Buffer interface {
	io.WriteCloser

	// CloseWithError closes and causes all subsequent Reads to return the error.
	CloseWithError(error)
	// NewReader returns a new reader for the Buffer. The ReadCloser must be closed when the caller is done reading.
	// The ReadCloser will automatically close after all bytes have been read.
	// A ReadCloser which has been closed will always return 0, io.EOF from Read.
	NewReader() io.ReadCloser
	ReadWriterCount() int
}

type buffer struct {
	fl *freeList

	blocks       []int
	lastBlockPos int // lastBlockPos is the last byte of the last block, in case the bytes written wasn't divisible by BlockSize.

	readWriterCount int
	closed          bool
	err             error
	m               sync.RWMutex
}

func (fl *freeList) NewBuffer() Buffer {
	return &buffer{
		fl:              fl,
		readWriterCount: 1,              // there is immediately 1 writer, until Close is called
		lastBlockPos:    fl.BlockSize(), // start at blocksize, so the first write allocates a new block
	}
}

func (bf *buffer) ReadWriterCount() int { return bf.readWriterCount }

var ErrClosedBuffer = errors.New("freelist: write on closed Buffer")

func (bf *buffer) Write(bts []byte) (int, error) {
	//	fmt.Printf("DEBUG buffer.Write called with '%v'\n", string(bts))

	bf.m.Lock()
	defer bf.m.Unlock()

	if bf.closed {
		return 0, ErrClosedBuffer
	}

	totalNumBytesCopied := 0
	for len(bts) > 0 {
		if bf.lastBlockPos == bf.fl.BlockSize() {
			blockI, err := bf.fl.getNewBlock()
			if err != nil {
				return 0, err
			}
			bf.blocks = append(bf.blocks, blockI)
			bf.lastBlockPos = 0
		}

		blockBytes := bf.fl.getBlockBytes(bf.blocks[len(bf.blocks)-1])
		blockBytes = blockBytes[bf.lastBlockPos:]

		numBytesCopied := copy(blockBytes, bts)

		bts = bts[numBytesCopied:]
		bf.lastBlockPos += numBytesCopied
		totalNumBytesCopied += numBytesCopied
	}

	return totalNumBytesCopied, nil
}

// Close always returns a nil error.
func (bf *buffer) Close() error {
	bf.m.Lock()
	defer bf.m.Unlock()
	if !bf.closed {
		bf.closed = true
		bf.readWriterCount--
	}
	return nil
}

func (bf *buffer) CloseWithError(err error) {
	//	fmt.Printf("DEBUG buffer.CloseWithError called %v\n", err)

	bf.m.Lock()
	defer bf.m.Unlock()
	if !bf.closed {
		bf.closed = true
		bf.readWriterCount--
	}
	bf.err = err
}

// Reader
////////////////////////////////////////////////////////////////////////////

func (bf *buffer) NewReader() io.ReadCloser {
	// TODO change count to an atomic var, cheaper than locking
	bf.m.Lock()
	defer bf.m.Unlock()
	bf.readWriterCount++
	return &reader{bf: bf}
}

type reader struct {
	bf       *buffer
	pos      int
	bfClosed bool // optimization, to avoid mutexing after the Buffer is closed to writes
	closed   bool // closed is whether this reader itself is closed (as opposed to bf being closed to new writes)
}

func (rr *reader) Read(bts []byte) (int, error) {
	//	fmt.Printf("DEBUG reader.Read called len(bts) %v\n", len(bts))
	if rr.closed {
		//		fmt.Printf("DEBUG reader.Read initially closed, returning EOF\n")
		return 0, io.EOF
	}

	totalBytesRead := 0
	for len(bts) > 0 {
		nextBufferBlockI := rr.pos / rr.bf.fl.blockSize

		if !rr.bfClosed {
			rr.bf.m.RLock()
		}
		nextBlockI := rr.bf.blocks[nextBufferBlockI]
		lenBlocks := len(rr.bf.blocks)
		closed := rr.bf.closed
		err := rr.bf.err
		if !rr.bfClosed {
			rr.bf.m.RUnlock()
		}

		if err != nil {
			//			fmt.Printf("DEBUG reader.Read error %v\n", err)
			return totalBytesRead, err
		}
		rr.bfClosed = closed

		onLastBlock := nextBufferBlockI == lenBlocks-1
		// if we're on the last block, sleep until either a new whole block is written, or Close() is called.
		if onLastBlock && !rr.bfClosed {
			time.Sleep(time.Millisecond * 10) // TODO change to a wakeup signal from Buffer.Write
			continue
		}

		blockLen := rr.bf.fl.BlockSize() // we only read full blocks, until Close() is called

		if onLastBlock {
			// note we don't mutex - once the buffer is closed to writes, mutexing is never required again
			blockLen = rr.bf.lastBlockPos
		}

		blockBytes := rr.bf.fl.getBlockBytes(nextBlockI)
		blockBytes = blockBytes[:blockLen] // trim, if we're in a last block

		// if this isn't the first read, and the previous read wasn't divisible by the block size, need to start after whatever was read.
		startOffset := rr.pos % rr.bf.fl.blockSize
		blockBytes = blockBytes[startOffset:]

		numBytesCopied := copy(bts, blockBytes)

		//		fmt.Printf("DEBUG reader.Read copying '%v'\n", string(bts[:numBytesCopied]))

		totalBytesRead += numBytesCopied
		bts = bts[numBytesCopied:]
		rr.pos += numBytesCopied // TODO only do once, right before returning (with totalBytesRead)

		if onLastBlock {
			break
		}
	}

	eofErr := error(nil)
	if rr.bfClosed && rr.pos == len(rr.bf.blocks)*rr.bf.fl.blockSize-(rr.bf.fl.blockSize-rr.bf.lastBlockPos) {
		// TODO fix: we aren't EOF just because the writer closed. Also need to be sure we're at the end, that bts wasn't too small
		eofErr = io.EOF
		rr.Close() // auto-close
	}
	return totalBytesRead, eofErr
}

func (rr *reader) Close() error {
	rr.bf.m.Lock()
	rr.bf.readWriterCount--
	defer rr.bf.m.Unlock()
	rr.closed = true
	return nil
}
