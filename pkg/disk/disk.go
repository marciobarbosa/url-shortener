// Disk
//
// Provides all the necessary abstractions to handle IOs.
package disk

import (
    "os"
    "sync"
    "errors"
    "syscall"
    "container/list"
)

type FileHdl struct {
    index int
    file *os.File
    size int64
    Fullpath string
    mutex sync.Mutex
}
var openfiles = list.New()
var index = 0
var Basepath = "./database"

// Open file, create a handle for it, and add this handle to the list of open
// files.
//
// Parameters:
//   filename: file name.
//
// Returns:
//   hdl: file handle.
//   error: error, if any.
func OpenFile(filename string) (*FileHdl, error) {
    var hdl FileHdl
    fullpath := Basepath + "/" + filename

    // If database directory doesn't exist, create it.
    if _, err := os.Stat(Basepath); errors.Is(err, os.ErrNotExist) {
	err := os.Mkdir(Basepath, os.ModePerm)
	if err != nil {
	    return nil, errors.New("Could not create base directory")
	}
    }

    flags := (os.O_RDWR | os.O_CREATE | os.O_APPEND)
    file, err := os.OpenFile(fullpath, flags, 0755)
    if err != nil {
	return nil, errors.New("Could not create file")
    }

    fi, err := os.Stat(fullpath)
    if err != nil {
	return nil, errors.New("Could not stat file")
    }

    hdl.index = index
    hdl.Fullpath = fullpath
    hdl.file = file;
    hdl.size = fi.Size();
    index++;

    openfiles.PushBack(&hdl)
    return &hdl, nil
}

// Append data to the end of the file.
//
// Parameters:
//   hdl: handle associated with file where data will be appended.
//   data: data to be appended.
//
// Returns:
//   offset: offset where the data was added.
//   error: error, if any.
func AppendData(hdl *FileHdl, data []byte) (int64, error) {
    hdl.mutex.Lock()
    offset := hdl.size
    nbytes, err := hdl.file.Write(data)

    if err != nil {
	hdl.mutex.Unlock()
	return -1, errors.New("Could not append data to the file")
    }
    hdl.file.Sync()
    hdl.size += int64(nbytes);
    hdl.mutex.Unlock()

    return offset, nil
}

// Read blob from a given file and offset.
//
// Parameters:
//   hdl: handle associated with file where data will be read.
//   offset: offset from where to start reading.
//   size: number of bytes to be read.
//
// Returns:
//   buffer: buffer holding the blob read from the file.
//   nbytes: number of bytes read.
func ReadData(hdl *FileHdl, offset uint64, size uint64) ([]byte, int) {
    buffer := make([]byte, size)
    unixfd := hdl.file.Fd()

    nbytes, err := syscall.Pread(int(unixfd), buffer, int64(offset))
    if err != nil {
	return nil, -1
    }
    return buffer, nbytes
}

// Close file and remove it from the list of open files.
//
// Parameters:
//   hdl: handle associated with file to be closed.
func CloseFile(hdl *FileHdl) {
    var next *list.Element

    for file_it := openfiles.Front(); file_it != nil; file_it = next {
	next = file_it.Next()
	file := file_it.Value.(*FileHdl)
	if file.index == hdl.index {
	    openfiles.Remove(file_it)
	}
    }
    hdl.file.Close()
}

// Close file, remove it from the list of open files, and delete it.
//
// Parameters:
//   hdl: handle associated with file to be removed.
func RemoveFile(hdl *FileHdl) {
    CloseFile(hdl)
    os.Remove(hdl.Fullpath)
}

// Truncate file.
//
// Parameters:
//   hdl: handle associated with file to be truncated.
func TruncateFile(hdl *FileHdl) {
    hdl.file.Truncate(0)
    hdl.file.Seek(0, 0)
}

// Close all open files.
func CloseAllFiles() {
    var next *list.Element

    for file_it := openfiles.Front(); file_it != nil; file_it = next {
	next = file_it.Next()
	file := file_it.Value.(*FileHdl)
	CloseFile(file)
    }
}
