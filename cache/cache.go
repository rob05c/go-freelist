package cache

import (
	"errors"
	"sync"
	"time"

	"github.com/apache/trafficcontrol/grove/lru"

	"github.com/rob05c/go-freelist"
)

type Cache struct {
	sizeBytes      uint64
	blockSizeBytes uint64
	fl             freelist.FreeList
	lru            *lru.LRU             // TODO remove LRU mutex. Since LRU access is already mutexed.
	buffers        map[string]*CacheObj // map[cacheKey]*CacheObj
	// lruBuffersM synchronizes lru and buffers. They MUST be synchronized together, and reads or writes of either must lock this.
	lruBuffersM *sync.RWMutex // TODO change to Mutex? Peek and Keys are the only Readlocks
}

var ErrNothingToRemove = errors.New("no buffers")
var ErrNoUnreadBuffers = errors.New("could not get an unread buffer")

var MaxBufferReturnTries = 1000
var BufferBlockSleepInterval = time.Millisecond * 10
var MaxBufferBlockTries = 100 * 10

func New(sizeBytes uint64, blockSizeBytes uint64) *Cache {
	cache := &Cache{
		sizeBytes:      sizeBytes,
		blockSizeBytes: blockSizeBytes,
		buffers:        map[string]*CacheObj{},
		lru:            lru.NewLRU(),
		lruBuffersM:    &sync.RWMutex{},
	}

	returnBuffer := func() (freelist.Buffer, error) {
		buf := (*CacheObj)(nil)
		tries := 0 // only try N times
		for {
			cache.lruBuffersM.Lock()
			oldestKey, _, ok := cache.lru.RemoveOldest() // we don't use the size
			if !ok {
				cache.lruBuffersM.Unlock()
				return nil, ErrNothingToRemove
			}
			buf, _ := cache.buffers[oldestKey] // TODO check ok? Should always exist.
			delete(cache.buffers, oldestKey)
			cache.lruBuffersM.Unlock()

			if buf.Buf.ReadWriterCount() > 0 {
				// If it's still being read, put it back in and try again
				cache.lruBuffersM.Lock()
				cache.buffers[oldestKey] = buf
				// TODO Add AddAsSecondOldest func, or similar? We really don't want to bump an old thing to the top.
				//      But on the other hand, if it's currently being read, maybe that's ok? This should be rare.
				cache.lru.Add(oldestKey, 0)
				cache.lruBuffersM.Unlock()

				tries++
				if tries > MaxBufferReturnTries {
					break
				}
				continue
			}
		}

		if buf == nil {
			// This means we tried the oldest MaxBufferReturnTries buffers, and they were all being read.
			// So, go ahead and grab the oldest, and hang onto it, and keep trying until it's no longer being read.

			// TODO log. This should be extremely rare, and is very bad if it's not.

			cache.lruBuffersM.Lock()
			oldestKey, _, ok := cache.lru.RemoveOldest() // we don't use the size
			if !ok {
				cache.lruBuffersM.Unlock()
				return nil, ErrNothingToRemove
			}
			buf, _ = cache.buffers[oldestKey] // TODO check ok? Should always exist
			delete(cache.buffers, oldestKey)
			cache.lruBuffersM.Unlock()

			// Ok, our buffer is now removed from the LRU and list of buffers, nothing should ever get it again.
			// Now, block until it's no longer being read.
			// TODO log. This should be extremely rare, but will happen forever if there's a bug and something doesn't close a reader.

			tries := 0
			for buf.Buf.ReadWriterCount() > 0 {
				tries++
				if tries > MaxBufferBlockTries {
					return nil, ErrNoUnreadBuffers
				}
				time.Sleep(BufferBlockSleepInterval)
			}
		}
		return buf.Buf, nil
	}

	cache.fl = freelist.New(int(sizeBytes), int(blockSizeBytes), returnBuffer)

	return cache
}

// CacheObj is an object stored in the Cache.
// It contains a Buf for bytes reading and writing,
// And a MetaData for structured non-freelist metadata.
//
// TODO change to only allow access to buffer.NewReader, so only the NewObj caller can Write.
type CacheObj struct {
	// Buf is the bytes storage of the cache, which uses the FreeList to reduce GC load.
	Buf freelist.Buffer
	// MetaData may be any metadata necessary. This will be stored, but does not use freelist.FreeList.
	// Therefore, all data possible should be stored in Buf.
	// Only data that absolutely requires nonlinear access by the application itself should be stored in MetaData.
	//
	// TODO verify this doesn't need to be a pointer
	MetaData interface{}
	HitCount uint64 // the number of times this object was requested from the cache
}

// Capacity returns the capacity of the cache in bytes.
func (ca *Cache) Capacity() uint64 { return ca.sizeBytes }

// NewObject returns a new object for reading and writing.
// The object will be immediately stored in the Cache, and is immediately available to clients calling Get.
//
// Note this is synchronized with Get. That is, if two goroutines simultaneously call NewObject and Get
//
// However, be aware there is a race condition, if a reader comes in at the exact same time NewObject is called,
// which will result in multiple clients thinking there is no object in the cache and fetching from the origin.
//
// Therefore, callers should synchronize NewObject and Get calls.
// They probably also need to synchronize the full "Get, then check if the object can be reused, and if not create a new object".
//
// There is also no synchronization around whatever is put in MetaData.
// Therefore, callers must do their own synchronization if whatever is put in CacheObj.MetaData
// is read or written by multiple goroutines.
//
func (ca *Cache) NewObj(key string) *CacheObj {
	obj := &CacheObj{
		Buf: ca.fl.NewBuffer(),
	}
	ca.lruBuffersM.Lock()
	ca.buffers[key] = obj
	ca.lru.Add(key, 0) // size is not used
	ca.lruBuffersM.Unlock()
	return obj
}

// LoadOrStore gets the CacheObj for key, or creates a new one if it doesn't exist.
// Returns the object, and whether it was loaded.
// The fetching and creation is atomic for the given key.
func (ca *Cache) LoadOrStore(key string) (*CacheObj, bool) {
	ca.lruBuffersM.Lock()
	if obj, ok := ca.buffers[key]; ok {
		ca.lru.Add(key, 0) // bump to the top of the LRU. size is not used
		ca.lruBuffersM.Unlock()
		return obj, true
	}

	obj := &CacheObj{
		Buf: ca.fl.NewBuffer(),
	}
	ca.buffers[key] = obj
	ca.lru.Add(key, 0) // size is not used
	ca.lruBuffersM.Unlock()
	return obj, false
}

// Get returns the object and whether it exists. The object is bumped to the top of the LRU.
func (ca *Cache) Get(key string) (*CacheObj, bool) {
	ca.lruBuffersM.Lock()
	obj, ok := ca.buffers[key]
	if !ok {
		ca.lruBuffersM.Unlock()
		return nil, false // TODO determine if we can avoid this if-check. I don't think so?
	}
	ca.lruBuffersM.Unlock()
	ca.lru.Add(key, 0) // bump to the top of the LRU. size is not used
	return obj, true
}

// Peek returns the object and whether it exists, without bumping the object to the top of the LRU.
func (ca *Cache) Peek(key string) (*CacheObj, bool) {
	ca.lruBuffersM.RLock()
	obj, ok := ca.buffers[key]
	ca.lruBuffersM.RUnlock()
	if !ok {
		return nil, false // TODO determine if we can avoid this if-check. I don't think so?
	}
	return obj, true
}

// Keys returns the keys in the cache.
func (ca *Cache) Keys() []string {
	ca.lruBuffersM.RLock()
	keys := ca.lru.Keys()
	ca.lruBuffersM.RUnlock()
	return keys
}

// Size always returns the capacity. The FreeList pre-allocates, and determining the used block bytes can't be done quickly.
func (ca *Cache) Size() uint64 { return ca.sizeBytes }

// Close implements io.Closer and other interfaces.
func (ca *Cache) Close() {}
