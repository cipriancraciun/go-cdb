

package cdb


import "errors"
import "os"
import "syscall"




// Open opens an existing CDB database at the given path, using `mmap`-ed memory.
func OpenMmap (_path string) (*CDB, error) {
	if _file, _error := os.Open (_path); _error == nil {
		return NewFromMappedWithHasher (_file, nil)
	} else {
		return nil, _error
	}
}


func NewFromMappedWithHasher (_file *os.File, _hasher func ([]byte) (uint32)) (*CDB, error) {
	var _size int64
	if _stat, _error := _file.Stat (); _error == nil {
		_size = _stat.Size ()
	} else {
		return nil, _error
	}
	if _size < 1024 {
		return nil, errors.New ("file is too small (or empty)")
	}
	if _size >= (4 * 1024 * 1024 * 1024) {
		return nil, errors.New ("file is too large")
	}
	var _data []byte
	if _data_0, _error := syscall.Mmap (int (_file.Fd ()), 0, int (_size), syscall.PROT_READ, syscall.MAP_SHARED); _error == nil {
		_data = _data_0
	} else {
		return nil, _error
	}
	_cdb, _error := NewFromBufferWithHasher (_data, _hasher)
	if _error != nil {
		_cdb.readerCloser = & mmapCloser { data : _data }
	}
	return _cdb, _error
}




type mmapCloser struct {
	data []byte
}


func (_closer *mmapCloser) Close () (error) {
	if _closer.data != nil {
		_error := syscall.Munmap (_closer.data)
		_closer.data = nil
		return _error
	} else {
		return nil
	}
}

