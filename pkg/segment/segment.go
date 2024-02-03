// Segment
//
// Provides all the necessary abstractions to handle Segments.
package segment

import (
    "fmt"
    "os"
    "sync"
    "errors"
    "strconv"
    "container/list"
    "encoding/binary"
    "github.com/emirpasic/gods/utils"
    "github.com/emirpasic/gods/trees/binaryheap"
    "github.com/emirpasic/gods/trees/redblacktree"
    "github.com/marciobarbosa/url-shortener/pkg/disk"
)

// states
const (
    RUNNING int =  0
    MERGING     =  1
    LOADING      = 2
    DISTRIBUTING = 3
    OFF          = 4
    COPYING      = 5
)
var state = OFF

// stats - cache
var nhits uint64 = 0
var nfaults uint64 = 0

// stats - disk
var nmerges uint64 = 0
var nflushes uint64 = 0

// config
type InitParams struct {
    NSegs int
    CacheLen int
    BasePath string
    LogName string
    ClearCache func()
}

// segment entry
type entry_t struct {
    key []byte
    value []byte
    seg *segmenthdl
}

// context
type segctx struct {
    done bool
    offset uint64
    file *disk.FileHdl
}

// segment handle
type segmenthdl struct {
    index int
    file *disk.FileHdl
}

var segments = list.New()
var maxsegs int

var cache *redblacktree.Tree
var cachesize int
var cachelen  int

var segindex = 0
var segcounter = 0
var basepath = "./database"

var recovery = "recovery"
var distname = "dist"
var recfile *disk.FileHdl

var rwlock sync.RWMutex
var ClearCacheCallback func()

// index table entry
type indexentry struct {
    keylen uint64
    valuelen uint64
    offset uint64
    segindex int
}
var indextable map[string]indexentry

// Populate index table from given segment file.
//
// Parameters:
//   seg: segment handle.
//
// Returns:
//   error: error, if any.
func _LoadSegment(seg *segmenthdl) (error) {
    var offset uint64 = 0
    var intsize uint64 = 8
    var ientry indexentry
    var oset uint64 = 0

    for {
	ksizeblob, nbytes := disk.ReadData(seg.file, offset, intsize)
	if nbytes <= 0 {
	    return nil
	}
	offset += uint64(nbytes)

	keysize := binary.LittleEndian.Uint64(ksizeblob)
	kblob, nbytes := disk.ReadData(seg.file, offset, keysize)
	offset += uint64(nbytes)

	vsizeblob, nbytes := disk.ReadData(seg.file, offset, intsize)
	offset += uint64(nbytes)
	valsize := binary.LittleEndian.Uint64(vsizeblob)
	offset += valsize

	ientry.keylen = keysize
	ientry.valuelen = valsize
	ientry.offset = oset
	ientry.segindex = seg.index
	indextable[string(kblob)] = ientry;

	oset = offset
    }
}

// Populate index table from existing segment files.
//
// Returns:
//   error: error, if any.
func _PopulateIndexTable() (error) {
    indextable = make(map[string]indexentry)

    for seg_it := segments.Front(); seg_it != nil; seg_it = seg_it.Next() {
	seg := seg_it.Value.(*segmenthdl)
	err := _LoadSegment(seg)
	if err != nil {
	    return err
	}
    }
    return nil
}

// Executed after a crash. This function reads the recovery file and re-add the
// entries that were not in a segment file at the time of the crash.
//
// Returns:
//   error: error, if any.
func _RecoverFromCrash() (error) {
    var offset uint64 = 0
    var intsize uint64 = 8
    var index int = 0
    var rfile *disk.FileHdl
    var nfiles int = 0
    var fullpath = basepath + "/" + recovery

    if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
	err := os.Mkdir(fullpath, os.ModePerm)
	if err != nil {
	    return err
	}
    }

    dir, err := os.ReadDir(fullpath)
    if err != nil {
	return err
    }

    for _, rec := range dir {
	file, err := disk.OpenFile(recovery + "/" + rec.Name())
	if err != nil {
	    disk.CloseAllFiles()
	    return err
	}

	index, err = strconv.Atoi(rec.Name())
	if err != nil {
	    disk.CloseAllFiles()
	    return err
	}
	rfile = file
	nfiles++
    }

    if nfiles == 0 {
	recfile, err = disk.OpenFile(recovery + "/" + "0")
	return err
    }
    if nfiles != 1 {
	disk.CloseAllFiles()
	return errors.New("More than one recovery file")
    }
    index++

    recfile, err = disk.OpenFile(recovery + "/" + strconv.Itoa(index))
    if err != nil {
	disk.CloseAllFiles()
	return err
    }

    for {
	ksizeblob, nbytes := disk.ReadData(rfile, offset, intsize)
	if nbytes <= 0 {
	    disk.RemoveFile(rfile)
	    return nil
	}
	offset += uint64(nbytes)

	keysize := binary.LittleEndian.Uint64(ksizeblob)
	kblob, nbytes := disk.ReadData(rfile, offset, keysize)
	offset += uint64(nbytes)

	vsizeblob, nbytes := disk.ReadData(rfile, offset, intsize)
	offset += uint64(nbytes)
	valsize := binary.LittleEndian.Uint64(vsizeblob)

	data, nbytes := disk.ReadData(rfile, offset, valsize)
	offset += uint64(nbytes)

	_, err := _Put(kblob, data)
	if err != nil {
	    return err
	}
    }
    return nil
}

// Load any existing segment file and use them to populate the index table.
// Also, create a new segment file for new entries, initialize the cache, and
// load entries from recovery file.
//
// Parameters:
//  params: config
//
// Returns:
//   error: error, if any.
func Init(params InitParams) (error) {
    segcounter = 0
    maxsegs = params.NSegs
    ClearCacheCallback = params.ClearCache

    basepath = params.BasePath
    disk.Basepath = basepath

    dir, err := os.ReadDir(basepath)
    if err != nil && !errors.Is(err, os.ErrNotExist) {
	return err
    }

    for _, seg := range dir {
	var seghdl segmenthdl

	if (seg.Name() == recovery) {
	    continue
	}
	if (seg.Name() == params.LogName) {
	    continue
	}
	if (seg.Name() == distname) {
	    continue
	}

	file, err := disk.OpenFile(seg.Name())
	if err != nil {
	    disk.CloseAllFiles()
	    return err
	}

	seghdl.index, err = strconv.Atoi(seg.Name())
	if err != nil {
	    disk.CloseAllFiles()
	    return err
	}
	seghdl.file = file
	segments.PushBack(&seghdl)

	segindex = seghdl.index;
	segcounter++
    }

    if segcounter > maxsegs {
	return errors.New("Segment exceeds the limit")
    }

    segindex++
    _PopulateIndexTable()
    _CreateSegment()

    cache = redblacktree.NewWithStringComparator()
    cachesize = params.CacheLen
    cachelen = 0

    err = _RecoverFromCrash()
    if err != nil {
	return err
    }

    state = RUNNING
    return nil;
}

// Flush entry into the given segment file. Also, update the index table so new
// requests can find this entry in the correct segment file.
//
// Parameters:
//  seg: segment file
//  key: key
//  value: value
//
// Returns:
//   error: error, if any.
func _FlushEntry(seg *segmenthdl, key []byte, value []byte) (error) {
    var ientry indexentry

    intsize := 8
    keysize := len(key)
    valuelen := len(value)

    if valuelen == 0 {
	// tombstone
	return nil
    }
    klen := make([]byte, intsize)
    binary.LittleEndian.PutUint64(klen, uint64(keysize))
    keyblob := append(klen, key...)

    vlen := make([]byte, intsize)
    binary.LittleEndian.PutUint64(vlen, uint64(valuelen))

    valblob := append(vlen, value...)
    data := append(keyblob, valblob...)
    offset, err := disk.AppendData(seg.file, data)

    ientry.keylen = uint64(keysize)
    ientry.valuelen = uint64(valuelen)
    ientry.offset = uint64(offset)
    ientry.segindex = seg.index
    indextable[string(key)] = ientry

    return err
}

// Merge all the existing segments into a single one. We compare the first
// entry of each sorted segment and add the smallest one to the new segment
// file. We repeat this process until we have merged all entries from all
// segment files into a single sorted segment file.
//
// Returns:
//   error: error, if any.
func _MergeSegments() (error) {
    var seg *segmenthdl
    var newseg segmenthdl
    var intsize uint64 = 8
    var prev *list.Element

    newfile, _ := disk.OpenFile(strconv.Itoa(segindex))
    newseg.index = segindex
    newseg.file = newfile

    segments.PushBack(&newseg)
    segindex++
    segcounter++

    ctx := make(map[int]segctx)

    for seg_it := segments.Back(); seg_it != nil; seg_it = seg_it.Prev() {
	seg = seg_it.Value.(*segmenthdl)
	sc := segctx{offset: 0, file: seg.file, done: false}
	ctx[seg.index] = sc
    }

    CompKeys := func(ent1, ent2 interface{}) int {
	e1 := ent1.(entry_t)
	e2 := ent2.(entry_t)
	return utils.StringComparator(string(e1.key), string(e2.key))
    }
    heap := binaryheap.NewWith(CompKeys)

    for {
	for seg_it := segments.Back().Prev(); seg_it != nil; seg_it = prev {
	    seg = seg_it.Value.(*segmenthdl)
	    sc := ctx[seg.index]
	    offset := sc.offset

	    ksizeblob, nbytes := disk.ReadData(seg.file, offset, intsize)
	    if nbytes <= 0 {
		prev = seg_it.Prev()
		segments.Remove(seg_it)
		sc.done = true
		ctx[seg.index] = sc
		disk.RemoveFile(seg.file)
		continue
	    }
	    offset += uint64(nbytes)

	    keysize := binary.LittleEndian.Uint64(ksizeblob)
	    kblob, nbytes := disk.ReadData(seg.file, offset, keysize)
	    offset += uint64(nbytes)

	    vsizeblob, nbytes := disk.ReadData(seg.file, offset, intsize)
	    offset += uint64(nbytes)
	    valsize := binary.LittleEndian.Uint64(vsizeblob)

	    data, nbytes := disk.ReadData(seg.file, offset, valsize)
	    offset += uint64(nbytes)

	    sc.offset = offset
	    ctx[seg.index] = sc
	    entry := entry_t{key: kblob, value: data, seg: seg}
	    heap.Push(entry)
	    prev = seg_it.Prev()
	}

	if segments.Len() == 1 {
	    break
	}

	min, _ := heap.Pop()
	entry := min.(entry_t)

	// remove duplicates
	lastkey := entry.key
	maxseg := entry.seg.index
	for {
	    min, found := heap.Peek()
	    if !found {
		break;
	    }
	    nextentry := min.(entry_t)
	    if string(nextentry.key) != string(lastkey) {
		break;
	    }
	    if nextentry.seg.index > maxseg {
		entry = nextentry
		maxseg = nextentry.seg.index
	    }
	    heap.Pop()
	}
	_ = _FlushEntry(&newseg, entry.key, entry.value)
    }

    // flush remaining entries
    for {
	min, found := heap.Pop()
	if !found {
	    break;
	}
	entry := min.(entry_t)
	lastkey := entry.key
	maxseg := entry.seg.index

	for {
	    min, found := heap.Peek()
	    if !found {
		break;
	    }
	    nextentry := min.(entry_t)
	    if string(nextentry.key) != string(lastkey) {
		break;
	    }
	    if nextentry.seg.index > maxseg {
		entry = nextentry
		maxseg = nextentry.seg.index
	    }
	    heap.Pop()
	}
	_ = _FlushEntry(&newseg, entry.key, entry.value)
    }
    segcounter = 1
    return nil
}

// Create new segment file. If the number of segments exceeds the maximum number
// of segments allowed, merge them all into a single segment.
func _CreateSegment() {
    var seghdl segmenthdl

    if segcounter >= maxsegs {
	nmerges++
	_ = _MergeSegments()
    }

    file, _ := disk.OpenFile(strconv.Itoa(segindex))
    seghdl.index = segindex
    seghdl.file = file

    segments.PushBack(&seghdl)
    segindex++
    segcounter++
}

// Add entry into the recovery file. This file is not sorted and is only used
// when recovering from a crash. It holds entries that haven't been stored in a
// segment file yet.
//
// Parameters:
//   key: key.
//   value: value.
//
// Returns:
//   error: error, if any.
func _RecoverLogAdd(key []byte, value []byte) (error) {
    intsize := 8
    keysize := len(key)
    valuelen := len(value)

    klen := make([]byte, intsize)
    binary.LittleEndian.PutUint64(klen, uint64(keysize))
    keyblob := append(klen, key...)

    vlen := make([]byte, intsize)
    binary.LittleEndian.PutUint64(vlen, uint64(valuelen))

    valblob := append(vlen, value...)
    data := append(keyblob, valblob...)

    _, err := disk.AppendData(recfile, data)
    return err
}

// Add entry to the current segment file and update the index table so this
// entry can be found.
//
// Parameters:
//   key: key.
//   value: value.
//
// Returns:
//   error: error, if any.
func PutDisk(key []byte, value []byte) (error) {
    var ientry indexentry

    intsize := 8
    keysize := len(key)
    valuelen := len(value)

    seg_it := segments.Back()
    seg := seg_it.Value.(*segmenthdl)

    klen := make([]byte, intsize)
    binary.LittleEndian.PutUint64(klen, uint64(keysize))
    keyblob := append(klen, key...)

    vlen := make([]byte, intsize)
    binary.LittleEndian.PutUint64(vlen, uint64(valuelen))

    valblob := append(vlen, value...)
    data := append(keyblob, valblob...)

    offset, err := disk.AppendData(seg.file, data)

    ientry.keylen = uint64(keysize)
    ientry.valuelen = uint64(valuelen)
    ientry.offset = uint64(offset)
    ientry.segindex = seg.index
    indextable[string(key)] = ientry

    return err
}

// Flush cache into the current segment file. Notice that the cache is a
// red-black tree and its entries are flushed in-order. As a result, the
// resulting segment file is sorted.
func FlushCache() {
    nflushes++
    keys := cache.Keys()
    values := cache.Values()

    for i := 0; i < len(keys); i++ {
	PutDisk([]byte(keys[i].(string)), values[i].([]byte))
    }
    disk.TruncateFile(recfile)
    cache.Clear()
    cachelen = 0
}

// Add entry to the cache and recovery file. If the size of the cache is
// exceeded, flush them all to the current segment file and create a new one.
//
// Parameters:
//   key: key of the entry to be added.
//   value: value of the entry to be added.
//
// Returns:
//   true: if entry already exists and has been updated.
//   false: if entry has been created.
//   error: error, if any.
func _Put(key []byte, value []byte) (bool, error) {
    _, exists := cache.Get(string(key))
    if !exists {
	_, exists = indextable[string(key)]
    }
    if cache.Size() >= cachesize {
	FlushCache()
	_CreateSegment()
    }
    _RecoverLogAdd(key, value)
    cache.Put(string(key), value)
    cachelen++

    return exists, nil
}

func Put(key []byte, value []byte) (bool, error) {
    rwlock.Lock()
    if state == OFF {
	rwlock.Unlock()
	return false, errors.New("Offline")
    }
    if state != RUNNING {
	rwlock.Unlock()
	return false, errors.New("Busy")
    }
    exists, _ := _Put(key, value)
    rwlock.Unlock()
    return exists, nil
}

// Delete a given entry. Instead of removing this entry from the database, we
// just add a tombstone for the key associated with this entry so readers know
// that it has been removed. Those tombstones are removed when the segment files
// are merged.
func Del(key []byte) (bool, error) {
    // add tombstone
    return Put(key, nil)
}

// Get entry from segment file. We use the index table to find in which segment
// file and offset this entry is stored.
//
// Parameters:
//   key: key of the entry to be retrieved.
//
// Returns:
//   entry: requested entry.
//   error: error, if any.
func GetDisk(key []byte) ([]byte, error) {
    var seg *segmenthdl

    ksize := indextable[string(key)].keylen
    vsize := indextable[string(key)].valuelen
    offset := indextable[string(key)].offset
    segindex := indextable[string(key)].segindex

    if vsize == 0 {
	// tombstone
	return nil, errors.New("Entry not found")
    }

    found := false
    for seg_it := segments.Back(); seg_it != nil; seg_it = seg_it.Prev() {
	seg = seg_it.Value.(*segmenthdl)
	if seg.index == segindex {
	    found = true
	    goto done
	}
    }
    if found == false {
	return nil, errors.New("Segment not found")
    }
  done:
    offset += (2 * 8) + ksize // skip header

    data, nbytes := disk.ReadData(seg.file, offset, vsize)
    if nbytes <= 0 {
	return nil, errors.New("Could not get key")
    }
    return data, nil
}

// Get entry from cache first. If not found, check if this entry can be found
// in a segment file.
//
// Parameters:
//   key: key of the entry to be retrieved.
//
// Returns:
//   entry: requested entry.
//   error: error, if any.
func Get(key []byte) ([]byte, error) {
    rwlock.RLock()
    if state == OFF {
	rwlock.RUnlock()
	return nil, errors.New("Offline")
    }
    centry, found := cache.Get(string(key))
    if !found {
	nfaults++
	data, err := GetDisk(key)
	rwlock.RUnlock()
	return data, err
    }
    nhits++
    data := centry.([]byte)
    if len(data) == 0 {
	// tombstone
	rwlock.RUnlock()
	return nil, errors.New("Entry not found")
    }
    rwlock.RUnlock()
    return data, nil
}

// Load segment file received from other server.
//
// Returns:
//   error: error, if any.
func LoadDist() (error) {
    var seghdl segmenthdl

    rwlock.Lock()
    state = LOADING

    FlushCache()
    segname := strconv.Itoa(segindex)
    _ = os.Rename(basepath + "/dist", basepath + "/" + segname)

    file, _ := disk.OpenFile(segname)
    seghdl.index = segindex
    seghdl.file = file

    segments.PushBack(&seghdl)
    segindex++
    segcounter++

    _LoadSegment(&seghdl)
    _CreateSegment()

    ClearCacheCallback()
    state = RUNNING
    rwlock.Unlock()

    return nil
}

// Load segment file recovered from other server.
//
// Parameters:
//   dstserv: unused.
//   filename: name of segment to be recovered.
//
// Returns:
//   error: error, if any.
func Recover(dstserv string, filename string) (error) {
    rwlock.Lock()
    state = COPYING

    os.Link(basepath + "/" + filename, basepath + "/" + distname)

    state = RUNNING
    rwlock.Unlock()

    LoadDist()
    return nil
}

// Heartbeat remote procedure call.
//
// Parameters:
//   flags: unused.
//
// Returns:
//   n_entries: number of entries stored in this server.
//   error: error, if any.
func Ping(flags int) (int, error) {
    return cachelen + len(indextable), nil
}

// Stop this service.
func Stop() {
    var next *list.Element

    for seg_it := segments.Front(); seg_it != nil; seg_it = next {
	next = seg_it.Next()
	segments.Remove(seg_it)
    }
    disk.CloseAllFiles()
}

// The server is being turned-off. Flush the cache and sync the files.
func Shutdown() {
    rwlock.Lock()
    FlushCache()
    disk.CloseAllFiles()
    rwlock.Unlock()
}

// Print statistics
func _PrintStats() {
    fmt.Printf("nhits: %v\n", nhits)
    fmt.Printf("nfaults: %v\n", nfaults)
    fmt.Printf("nmerges: %v\n", nmerges)
    fmt.Printf("nflushes: %v\n", nflushes)
}
