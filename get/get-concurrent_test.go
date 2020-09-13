package get

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/rob05c/go-freelist/cache"
)

func TestConcurrentRead(t *testing.T) {
	blockSize := 5
	sizeBytes := 100 * BytesPerMebibyte
	cache := cache.New(uint64(sizeBytes), uint64(blockSize))

	getter := NewGetter(cache)

	inputFileName := `test_input.txt`
	// t.Logf("reading input file\n")
	inputBts, err := ioutil.ReadFile(inputFileName)
	// t.Logf("read input file\n")
	if err != nil {
		t.Fatalf("reading test file '%v': %v", inputFileName, err)
	}
	input := string(inputBts)

	// t.Logf("input  len %v\n", len(input))

	// t.Logf("getting mock getter\n")
	getFunc := GetHTTPMockTxt(input)
	// t.Logf("got mock getter\n")
	cacheKey := inputFileName

	doRead := func(outputChan chan string) {
		// t.Logf("doRead starting\n")
		reader, _, _, err := getter.Get(cacheKey, getFunc)
		// t.Logf("doRead got\n")
		if err != nil {
			t.Fatal("first get: " + err.Error())
		}
		defer reader.Close()

		allReader := []byte{}
		buf := make([]byte, 10, 10)
		for {
			// t.Logf("doRead reading\n")
			n, err := reader.Read(buf)
			// t.Logf("doRead read\n")
			allReader = append(allReader, buf[:n]...)
			if err != nil && err != io.EOF {
				t.Fatal("read: " + err.Error())
				return
			}
			if n == 0 || err == io.EOF {
				break
			}
		}
		// t.Logf("doRead writing output\n")
		outputChan <- string(allReader)
		// t.Logf("doRead returning\n")
	}

	concurrentReaders := 100

	// t.Logf("making output chans\n")

	outputChan := make(chan string, concurrentReaders)

	// t.Logf("running goroutines\n")
	for i := 0; i < concurrentReaders; i++ {
		go doRead(outputChan)
	}
	// t.Logf("started goroutines\n")

	outputs := []string{}
	for i := 0; i < concurrentReaders; i++ {
		// t.Logf("reading output\n")
		outputs = append(outputs, <-outputChan)
		// t.Logf("output len %v\n", len(outputs[len(outputs)-1]))
	}

	// t.Logf("read outputs\n")

	if outputs[0] != input {
		// not really fatal, but we don't want to spam the output
		t.Fatalf("expected input '''%v''' to equal output '''%v", input, outputs[0])
	}

	for i := 0; i < len(outputs)-1; i++ {
		if outputs[i] != outputs[i+1] {
			// not really fatal, but we don't want to spam the output
			t.Fatalf("expected first read '''%v''' to equal second read '''%v", outputs[i], outputs[i+1])
		}
	}
}
