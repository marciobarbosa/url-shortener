// Database
//
// Provides all the necessary abstractions to handle transactions.
package database

import (
    "fmt"
    "sync"
    "errors"
    "github.com/bluele/gcache"
    "github.com/marciobarbosa/url-shortener/pkg/segment"
)

// cache policies
const (
    LRU cachepolicy = 0
    LFU             = 1
)
type cachepolicy int

type Status int
const (
    CREATED Status = 0
    UPDATED        = 1
    FAILED         = 2
    STOPPED        = 3
    LOCKED         = 4
    FOUND          = 5
    DELETED        = 6
)

// stats
var nhits uint64 = 0
var nfaults uint64 = 0

// cache
var gc gcache.Cache

var csize int
var cpolicy cachepolicy
var rwlock sync.RWMutex

// segment layer default config
var nsegs int = 50
var segcachelen int = 2048

// parameters
type InitParams struct {
    Policy cachepolicy
    CacheSize int
    BasePath string
    LogName string
}

// Initialize the cache according to the specified policy.
//
// Returns:
//   error: error, if any.
func _InitCache() (error) {
    switch cpolicy {
    case LRU:
	gc = gcache.New(csize).LRU().Build()
    case LFU:
	gc = gcache.New(csize).LFU().Build()
    default:
	return errors.New("Policy not supported")
    }
    return nil
}

// Remove all entries from the cache.
func ClearCache() {
    switch cpolicy {
    case LRU:
	gc.Purge()
    case LFU:
	gc.Purge()
    }
}

// Initialize the database layer.
//
// Parameters:
//   params: config
//
// Returns:
//   error: error, if any.
func Init(params InitParams) (error) {
    var sparams segment.InitParams
    csize = params.CacheSize
    cpolicy = params.Policy

    err := _InitCache()
    if err != nil {
	return err
    }
    sparams.NSegs = nsegs
    sparams.CacheLen = segcachelen
    sparams.BasePath = params.BasePath
    sparams.LogName = params.LogName
    sparams.ClearCache = ClearCache

    err = segment.Init(sparams)
    if err != nil {
	return err
    }
    return nil
}

// Add entry to the cache.
//
// Parameters:
//   key: key associated with entry to be added.
//   entry: entry associated with entry to be added.
func _InsertEntry(key []byte, value []byte) {
    switch cpolicy {
    case LRU:
	gc.Set(string(key), value)
    case LFU:
	gc.Set(string(key), value)
    }
}

// Add entry to the database. In other words, add entry to the segment file
// first, then add it to the database cache.
//
// Parameters:
//   key: key associated with entry to be added.
//   entry: entry associated with entry to be added.
//
// Returns:
//   status: created, updated, stopped, or locked.
func Insert(key []byte, value []byte) (Status) {
    rwlock.Lock()
    var stat Status = CREATED

    exists, err := segment.Put(key, value)
    if err != nil {
	var errmsg string = err.Error()
	stat = FAILED
	if errmsg == "Offline" {
	    stat = STOPPED
	} else if errmsg == "Busy" {
	    stat = LOCKED
	}
	rwlock.Unlock()
	return stat
    }
    if exists {
	stat = UPDATED
    }
    _InsertEntry(key, value)

    rwlock.Unlock()
    return stat
}

// Get entry from the database cache.
//
// Parameters:
//   key: key associated with entry to be retrieved.
//
// Returns:
//   entry: entry associated with the given key.
func _GetEntry(key []byte) ([]byte, bool) {
    switch cpolicy {
    case LRU:
	entry, err := gc.Get(string(key))
	if err != nil {
	    return nil, false
	}
	return entry.([]byte), true
    case LFU:
	entry, err := gc.Get(string(key))
	if err != nil {
	    return nil, false
	}
	return entry.([]byte), true
    }
    return nil, false
}

// Get entry from the database. Check if it is present in the cache first. If
// not, get it from the segment layer and add it to the database cache.
//
// Parameters:
//   key: key associated with entry to be retrieved.
//
// Returns:
//   entry: entry associated with the given key.
//   status: found, failed, or stopped.
func Request(key []byte) ([]byte, Status) {
    var stat Status = FOUND
    rwlock.RLock()
    if data, found := _GetEntry(key); found {
	nhits++
	rwlock.RUnlock()
	return data, stat
    }
    data, err := segment.Get(key)
    if err != nil {
	var errmsg string = err.Error()
	stat = FAILED
	if errmsg == "Offline" {
	    stat = STOPPED
	}
	rwlock.RUnlock()
	return nil, stat
    }
    nfaults++
    rwlock.RUnlock()
    rwlock.Lock()
    _InsertEntry(key, data)
    rwlock.Unlock()
    return data, stat
}

// Remove entry from the database cache.
//
// Parameters:
//   key: key associated with entry to be removed.
func _RemoveEntry(key []byte) {
    switch cpolicy {
    case LRU:
	gc.Remove(string(key))
    case LFU:
	gc.Remove(string(key))
    }
}

// Remove entry from the database. Delete entry from the segment layer first,
// then remove it from the database cache.
//
// Parameters:
//   key: key associated with entry to be removed.
//
// Returns:
//   entry: entry associated with the given key.
//   status: deleted, failed, stopped, or locked.
func Remove(key []byte) ([]byte, Status) {
    rwlock.Lock()
    var stat Status = DELETED
    var errmsg string
    var data []byte
    var err error

    data, found := _GetEntry(key)
    if found {
	nhits++
    } else {
	nfaults++
	data, err = segment.Get(key)
	if err != nil {
	    errmsg = err.Error()
	    stat = FAILED
	    if errmsg == "Offline" {
		stat = STOPPED
	    }
	    rwlock.Unlock()
	    return nil, stat
	}
    }
    _, err = segment.Del(key)
    if err != nil {
	errmsg = err.Error()
	stat = FAILED
	if errmsg == "Offline" {
	    stat = STOPPED
	} else if errmsg == "Busy" {
	    stat = LOCKED
	}
	rwlock.Unlock()
	return nil, stat
    }
    _RemoveEntry(key)

    rwlock.Unlock()
    return data, stat
}

// Server is being turned-off.
func Shutdown() {
    segment.Shutdown()
}

// Print statistics
func _PrintStats() {
    fmt.Printf("nhits: %v\n", nhits)
    fmt.Printf("nfaults: %v\n", nfaults)
}
