package get

import (
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/rob05c/go-freelist/cache"
)

var BytesPerKibibyte = 1024
var BytesPerMebibyte = 1024 * BytesPerKibibyte
var RequestTimeout = time.Second * 5

func TestSimpleFetch(t *testing.T) {
	blockSize := 5
	sizeBytes := 100 * BytesPerMebibyte
	cache := cache.New(uint64(sizeBytes), uint64(blockSize))

	getter := NewGetter(cache)

	client := &http.Client{
		Timeout: RequestTimeout,
	}

	urlStr := "http://example.com"

	// getFunc := GetHTTP(urlStr, client)
	getFunc := GetHTTPMock(urlStr, client)

	cacheKey := urlStr

	reader, metaData, wasCached, err := getter.Get(cacheKey, getFunc)
	if err != nil {
		t.Fatal("first get: " + err.Error())
	}
	defer reader.Close()
	bts, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal("first get: reading: " + err.Error())
	}
	reader.Close() // close immediately, don't defer, so the second read finishes. Close should be safe for multiple calls
	// 	t.Logf(`First read got:
	// time:	%v
	// code:	%v
	// cached:	%v
	// blen:	%v
	// bhead:	'%v'

	// `, metaData.ReqTime,
	// 		metaData.Code,
	// 		wasCached,
	// 		len(bts),
	// 		string(bts[:10]),
	// 	)

	reader2, metaData2, wasCached2, err := getter.Get(cacheKey, getFunc)
	if err != nil {
		t.Fatal("second get: " + err.Error())
	}
	defer reader2.Close()

	bts2, err := ioutil.ReadAll(reader2)
	if err != nil {
		t.Fatal("second get: reading: " + err.Error())
	}
	reader2.Close() // close immediately, don't defer, so the second read finishes. Close should be safe for multiple calls
	// 	t.Logf(`Second read got:
	// time:	%v
	// code:	%v
	// cached:	%v
	// blen:	%v
	// bhead:	'%v'

	// `, metaData2.ReqTime,
	// 		metaData2.Code,
	// 		wasCached2,
	// 		len(bts2),
	// 		string(bts2[:10]),
	// 	)

	btsStr := string(bts)
	bts2Str := string(bts2)
	if btsStr != bts2Str {
		t.Errorf("Expected first read '''%v''' to equal second read '''%v'''", btsStr, bts2Str)
	}
	if metaData.Code != metaData2.Code {
		t.Errorf("Expected first read code %v to equal second read code %v", metaData.Code, metaData2.Code)
	}
	if wasCached {
		t.Errorf("Expected first read to not be cached")
	}
	if !wasCached2 {
		t.Errorf("Expected second read to be cached")
	}
}

func TestInterleavedRead(t *testing.T) {
	blockSize := 5
	sizeBytes := 100 * BytesPerMebibyte
	cache := cache.New(uint64(sizeBytes), uint64(blockSize))

	getter := NewGetter(cache)

	client := &http.Client{
		Timeout: RequestTimeout,
	}

	urlStr := "http://example.com"

	//	getFunc := GetHTTP(urlStr, client)
	getFunc := GetHTTPMock(urlStr, client)

	cacheKey := urlStr

	reader, metaData, wasCached, err := getter.Get(cacheKey, getFunc)
	if err != nil {
		t.Fatal("first get: " + err.Error())
	}
	defer reader.Close()

	if metaData.Code != 200 {
		t.Errorf("Expected first read code %v to be 200", metaData.Code)
	}
	if wasCached {
		t.Errorf("Expected first read to not be cached")
	}

	// 	t.Logf(`First read got:
	// time:	%v
	// code:	%v
	// cached:	%v

	// `, metaData.ReqTime,
	// 		metaData.Code,
	// 		wasCached,
	// 	)

	reader2, metaData2, wasCached2, err2 := getter.Get(cacheKey, getFunc)
	if err2 != nil {
		t.Fatal("second get: " + err.Error())
	}
	defer reader2.Close()

	if metaData2.Code != 200 {
		t.Errorf("Expected second read code %v to be 200", metaData2.Code)
	}
	if !wasCached2 {
		t.Errorf("Expected second read to be cached")
	}

	// 	t.Logf(`Second read got:
	// time:	%v
	// code:	%v
	// cached:	%v

	// `, metaData2.ReqTime,
	// 		metaData2.Code,
	// 		wasCached2,
	// 	)

	allReader := []byte{}
	allReader2 := []byte{}

	buf := make([]byte, 10, 10)
	buf2 := make([]byte, 10, 10)

	// do an initial read of the first reader, so it's always ahead of the second
	n, err := reader.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal("read0: " + err.Error())
	}
	// t.Logf("rea00 '%v'\n", string(buf[:n]))
	allReader = append(allReader, buf[:n]...)

	readerClosed := false
	for {
		if !readerClosed { // we need to check, because reader will close before reader2
			n, err := reader.Read(buf)
			allReader = append(allReader, buf[:n]...)
			if err != nil && err != io.EOF {
				t.Fatal("read0: " + err.Error())
				return
			}
			if n == 0 || err == io.EOF {
				reader.Close() // need to close, so reader2 knows it's done.
				readerClosed = true
			}
		}

		// t.Logf("read0 '%v'\n", string(buf[:n]))

		n, err = reader2.Read(buf2)
		allReader2 = append(allReader2, buf2[:n]...)
		if err != nil && err != io.EOF {
			t.Fatal("read2: " + err.Error())
			return
		}

		// t.Logf("read2 '%v'\n", string(buf2[:n]))

		if n == 0 || err == io.EOF {
			break
		}
	}

	allReaderStr := string(allReader)
	allReader2Str := string(allReader2)
	if allReaderStr != allReader2Str {
		t.Errorf("expected first read '''%v''' to equal second read '''%v", allReaderStr, allReader2Str)
	}
	// t.Log("read All0: '''" + string(allReader) + "'''")
	// t.Log("read All2: '''" + string(allReader2) + "'''")
}
