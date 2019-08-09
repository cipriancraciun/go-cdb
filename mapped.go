

package cdb


import "errors"
import "os"
import "syscall"




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
	if _size >= (2 * 1024 * 1024 * 1024) {
		return nil, errors.New ("file is too large")
	}
	var _data []byte
	if _data_0, _error := syscall.Mmap (int (_file.Fd ()), 0, int (_size), syscall.PROT_READ, syscall.MAP_SHARED); _error == nil {
		_data = _data_0
	} else {
		return nil, _error
	}
	return NewFromBufferWithHasher (_data, nil)
}

