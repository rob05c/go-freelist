package freelist

import (
	"bytes"
	"io"
	"testing"
)

func TestNew(t *testing.T) {

	returnBuffer := func() (Buffer, error) {

		return nil, nil
	}

	sizeBytes := 10000
	blockSizeBytes := 1024
	fl := New(sizeBytes, blockSizeBytes, returnBuffer)
	if fl == nil {
		t.Fatalf("expected New FreeList not nil, actual nil")
	}

	realSizeBytes := blockSizeBytes * 10

	if fl.Size() != realSizeBytes {
		t.Errorf("expected size %v actual %v", realSizeBytes, fl.Size())
	}

	buf := fl.NewBuffer()
	if buf == nil {
		t.Fatalf("expected FreeList.NewBuffer not nil, actual nil")
	}

	// test writing bytes

	bytesToWrite := []byte("Hello, World!")

	bytesWritten, err := buf.Write(bytesToWrite)
	if err != nil {
		t.Fatalf("buffer.Write error expected nil, actual %v", err)
	}
	if bytesWritten != len(bytesToWrite) {
		t.Fatalf("expected Buffer.Write len %v actual %v", len(bytesToWrite), bytesWritten)
	}
	buf.Close()

	{
		// test reading bytes after they're written

		reader := buf.NewReader()
		if reader == nil {
			t.Fatalf("expected Buffer.NewReader not nil, actual nil")
		}

		readBytes := make([]byte, len(bytesToWrite))
		numReadBytes, err := reader.Read(readBytes)
		if err != io.EOF {
			t.Fatalf("Reader.Read error expected io.EOF, actual %v", err)
		}
		if numReadBytes != len(bytesToWrite) {
			t.Fatalf("expected buf.Read len %v actual %v", len(bytesToWrite), numReadBytes)
		}

		if !bytes.Equal(bytesToWrite, readBytes) {
			t.Errorf("expected buf.Read '%v' actual '%v'", string(bytesToWrite), string(readBytes))
		}

		if numBytesRead, err := reader.Read(readBytes); err != io.EOF {
			t.Errorf("expected Buffer.Read error after reading everything to be io.EOF, actual: %v", err)
		} else if numBytesRead != 0 {
			t.Errorf("expected Buffer.Read num after reading everything to be 0, actual: %v", numBytesRead)
		}
	}

	{
		// test that a second reader works

		reader := buf.NewReader()
		if reader == nil {
			t.Fatalf("expected second Buffer.NewReader not nil, actual nil")
		}

		// intentionally make it bigger than what we'll read, to make sure reading stops when the input to Read is bigger.
		readBytes := make([]byte, len(bytesToWrite)*2)
		numReadBytes, err := reader.Read(readBytes)
		if err != io.EOF {
			t.Fatalf("second Reader.Read error expected io.EOF, actual %v", err)
		}
		if numReadBytes != len(bytesToWrite) {
			t.Fatalf("expected second Buffer.Read len %v actual %v", len(bytesToWrite), numReadBytes)
		}

		readBytes = readBytes[:numReadBytes] // trim to the size of what we read
		if !bytes.Equal(bytesToWrite, readBytes) {
			t.Errorf("expected second Buffer.Read '%v' actual '%v'", string(bytesToWrite), string(readBytes))
		}

		if numBytesRead, err := reader.Read(readBytes); err != io.EOF {
			t.Errorf("expected Buffer.Read error after reading everything to be io.EOF, actual: %v", err)
		} else if numBytesRead != 0 {
			t.Errorf("expected Buffer.Read num after reading everything to be 0, actual: %v", numBytesRead)
		}
	}

	{
		// test reading into a buffer smaller than what we have to read

		reader := buf.NewReader()
		if reader == nil {
			t.Fatalf("expected second Buffer.NewReader not nil, actual nil")
		}

		byteReadCount := 0

		// intentionally make it smaller than what we'll read, to make sure reading into a smaller buffer works
		readBytes := make([]byte, 5)
		{
			numReadBytes, err := reader.Read(readBytes)
			if err != nil {
				t.Fatalf("first smaller Reader.Read error expected nil, actual %v", err)
			}
			if numReadBytes != len(readBytes) {
				t.Fatalf("expected first smaller Buffer.Read len %v actual %v", len(readBytes), numReadBytes)
			}

			bytesToWriteExpected := bytesToWrite[byteReadCount : byteReadCount+numReadBytes]

			if !bytes.Equal(bytesToWriteExpected, readBytes) {
				t.Errorf("expected first smaller Buffer.Read '%v' actual '%v'", string(bytesToWriteExpected), string(readBytes))
			}

			byteReadCount += numReadBytes
		}
		{
			numReadBytes, err := reader.Read(readBytes)
			if err != nil {
				t.Fatalf("second smaller Reader.Read error expected nil, actual %v", err)
			}
			if numReadBytes != len(readBytes) {
				t.Fatalf("expected second smaller Buffer.Read len %v actual %v", len(readBytes), numReadBytes)
			}

			bytesToWriteExpected := bytesToWrite[byteReadCount : byteReadCount+numReadBytes]

			if !bytes.Equal(bytesToWriteExpected, readBytes) {
				t.Errorf("expected second smaller Buffer.Read '%v' actual '%v'", string(bytesToWriteExpected), string(readBytes))
			}

			byteReadCount += numReadBytes
		}
		{
			numReadBytes, err := reader.Read(readBytes)
			if err != io.EOF {
				t.Fatalf("third smaller Reader.Read error expected io.EOF, actual %v", err)
			}
			if numReadBytes != 3 {
				t.Fatalf("expected third smaller Buffer.Read len %v actual %v", 3, numReadBytes)
			}

			bytesToWriteExpected := bytesToWrite[byteReadCount : byteReadCount+numReadBytes]
			readBytes = readBytes[:numReadBytes] // trim to read count

			if !bytes.Equal(bytesToWriteExpected, readBytes) {
				t.Errorf("expected third smaller Buffer.Read '%v' actual '%v'", string(bytesToWriteExpected), string(readBytes))
			}

			byteReadCount += numReadBytes
		}

	}

	// TODO test manual reader close before reading everything

	if bufPrivate := buf.(*buffer); bufPrivate.readWriterCount != 0 {
		t.Errorf("expected buffer readWriteCount 0, actual %v", bufPrivate.readWriterCount)
	}

}

func TestWriteLargerThanBlockSize(t *testing.T) {

	returnBuffer := func() (Buffer, error) {
		return nil, nil
	}

	sizeBytes := 10000
	blockSizeBytes := 10
	fl := New(sizeBytes, blockSizeBytes, returnBuffer)
	if fl == nil {
		t.Fatalf("expected New FreeList not nil, actual nil")
	}

	buf := fl.NewBuffer()
	if buf == nil {
		t.Fatalf("expected FreeList.NewBuffer not nil, actual nil")
	}

	// test writing bytes

	bytesToWrite := []byte(`
    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

    Curabitur pretium tincidunt lacus. Nulla gravida orci a odio. Nullam varius, turpis et commodo pharetra, est eros bibendum elit, nec luctus magna felis sollicitudin mauris. Integer in mauris eu nibh euismod gravida. Duis ac tellus et risus vulputate vehicula. Donec lobortis risus a elit. Etiam tempor. Ut ullamcorper, ligula eu tempor congue, eros est euismod turpis, id tincidunt sapien risus a quam. Maecenas fermentum consequat mi. Donec fermentum. Pellentesque malesuada nulla a mi. Duis sapien sem, aliquet nec, commodo eget, consequat quis, neque. Aliquam faucibus, elit ut dictum aliquet, felis nisl adipiscing sapien, sed malesuada diam lacus eget erat. Cras mollis scelerisque nunc. Nullam arcu. Aliquam consequat. Curabitur augue lorem, dapibus quis, laoreet et, pretium ac, nisi. Aenean magna nisl, mollis quis, molestie eu, feugiat in, orci. In hac habitasse platea dictumst.
`)

	bytesWritten, err := buf.Write(bytesToWrite)
	if err != nil {
		t.Fatalf("buffer.Write error expected nil, actual %v", err)
	}
	if bytesWritten != len(bytesToWrite) {
		t.Fatalf("expected Buffer.Write len %v actual %v", len(bytesToWrite), bytesWritten)
	}
	buf.Close()

	totalReadBuf := []byte{}

	readBuf := make([]byte, 5)

	reader := buf.NewReader()
	for {
		numReadBytes, err := reader.Read(readBuf)
		totalReadBuf = append(totalReadBuf, readBuf[:numReadBytes]...)
		if err == io.EOF {
			break
		}
	}

	if !bytes.Equal(bytesToWrite, totalReadBuf) {
		t.Errorf("expected multiple Buffer.Read to equal write '%v' actual '%v'", string(bytesToWrite), string(totalReadBuf))
	}

}
