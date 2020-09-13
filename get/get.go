package get

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rob05c/go-freelist"
	"github.com/rob05c/go-freelist/cache"
)

// CacheObjMetaData is the metadata required by the HTTP Cache.
// TODO move elsewhere
type CacheObjMetaData struct {
	ReqHeaders  http.Header
	RespHeaders http.Header
	//	RespCacheControl web.CacheControl
	Code         int
	OriginCode   int
	ProxyURL     string
	ReqTime      time.Time // our client's time when the object was requested
	ReqRespTime  time.Time // our client's time when the object was received
	RespRespTime time.Time // the origin server's Date time when the object was sent
	LastModified time.Time // the origin LastModified if it exists, or Date if it doesn't
	Size         uint64
	ReadErr      error
}

type Getter struct {
	//	m     *sync.Mutex
	keyLocks *sync.Map
	ca       *cache.Cache
}

func NewGetter(cache *cache.Cache) *Getter {
	return &Getter{
		//		m:     &sync.Mutex{},
		keyLocks: &sync.Map{},
		ca:       cache,
	}
}

type GetterFunc func() (io.ReadCloser, *CacheObjMetaData, error)

// Get gets the thing from the address.
// Gets it from the cache if it's in there. Otherwise, gets it from the source, putting it in the cache.
//
// Returns the bytes reader, the metadata, whether the object was cached, and any error.
//
// The getter should get the metadata, and immediately return the reader without actually reading.
//
// Note if get returns an error, the MetaData returned by get is returned from Get.
// Therefore, if get returns valid MetaData even on error, then Get will also return valid MetaData.
//
// Also note even if an error occurs, the key is kept in the cache, along with the MetaData get returns.
// It is the callers responsibility to remove errored objects from the cache.
// This allows for "negative caching, if the caller wants to keep errors with valid metadata in the cache
// for some time period.
//
// If get returns an error, then this returns a nil ReadCloser and that error.
//
// If get returns a valid readCloser and no error, then this returns a TeeReader which will write to the cache as it reads.
// Therefore, callers should be prompt about reading, because other concurrent requestors of the same key will be waiting
// until the first reader reads, which reads from the source.
//
// Readers will also block and never finish reading, until the returned reader is closed (which will close the cache Writer).
// Therefore, readers
//
func (gt *Getter) Get(key string, get GetterFunc) (io.ReadCloser, *CacheObjMetaData, bool, error) {
	// TODO consider changing to spawn a ReadAll in a goroutine? To avoid the "must read" scenario? Probably slower.

	// TODO remove keylock, add/use atomic GetOrNew
	iKeyLock, _ := gt.keyLocks.LoadOrStore(key, &sync.Mutex{})
	keyLock := iKeyLock.(*sync.Mutex)
	keyLock.Lock()

	obj, ok := gt.ca.Get(key)
	if ok {
		keyLock.Unlock()
		return obj.Buf.NewReader(), obj.MetaData.(*CacheObjMetaData), true, nil
	}
	obj = gt.ca.NewObj(key)
	readCloser, metaData, err := get()
	obj.MetaData = metaData
	keyLock.Unlock()
	if err != nil {
		return nil, metaData, false, err
	}

	return newTeeReader(readCloser, obj.Buf), metaData, false, nil
}

// newTeeReader takes a reader and the CacheObj freelist.Buffer, and writes what it reads to the buffer.
//
// When Close is called on the returned ReadCloser,
// if any bytes remain on the original closer, they are read into writer before calling Close on both rd and writer.
//
func newTeeReader(rd io.ReadCloser, writer freelist.Buffer) io.ReadCloser {
	return &teeReader{rd: rd, buf: writer}
}

type teeReader struct {
	rd  io.ReadCloser
	buf freelist.Buffer // TODO change to an interface that's io.WriterCloser and CloseWithError (no NewReader)
}

func (tr *teeReader) Read(bts []byte) (n int, err error) {
	n, err = tr.rd.Read(bts)
	if n > 0 {
		if n, err := tr.buf.Write(bts[:n]); err != nil {
			// TODO do we need to call writer.CloseWithError here?
			return n, err
		}
	}
	if err != nil {
		if err != io.EOF {
			tr.buf.CloseWithError(err)
		} else {
			tr.buf.Close() // freelist.Buffer.Close is documented to always return nil
		}
	}
	return
}

func (tr *teeReader) Close() error {
	// TODO add close atomic bool, to not do this stuff on double-closes?

	_, err := io.Copy(tr.buf, tr.rd)
	if err != nil {
		tr.buf.CloseWithError(err)
	} else {
		tr.buf.Close() // freelist.Buffer.Close is documented to always return nil
	}
	return tr.rd.Close()
}
