
package cdb

import (
	"unsafe"
)

// NOTE:  Based on => https://github.com/golang/go/blob/ecb2f231fa41b581319505139f8d5ac779763bee/src/runtime/stubs.go#L172-L181
//go:nosplit
func noEscape (p unsafe.Pointer) (unsafe.Pointer) {
	x := uintptr (p)
	return unsafe.Pointer (x ^ 0)
}

//go:nosplit
func NoEscapeBytes (_input *[]byte) (*[]byte) {
	return (*[]byte) (noEscape (unsafe.Pointer (_input)))
}

