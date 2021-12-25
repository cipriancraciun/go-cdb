
package cdb_test

import (
	"testing"
	"unsafe"

	"github.com/colinmarc/cdb"
	"github.com/stretchr/testify/require"
)


func BenchmarkStackEscapeNo(b *testing.B) {
	db, err := cdb.OpenMmap("./test/test.cdb")
	require.NoError(b, err)
	require.NotNil(b, db)

	for i := 0; i < b.N; i++ {
		keyOnStack := [2]byte {'X', byte (i)}
		keySlice := keyOnStack[:]
		keyNoEscape := *NoEscapeBytes (&keySlice)
		db.Get(keyNoEscape)
	}
}

func BenchmarkStackEscapeYes(b *testing.B) {
	db, err := cdb.OpenMmap("./test/test.cdb")
	require.NoError(b, err)
	require.NotNil(b, db)

	for i := 0; i < b.N; i++ {
		keyOnStack := [2]byte {'X', byte (i)}
		keySlice := keyOnStack[:]
		db.Get(keySlice)
	}
}




//go:nosplit
func NoEscape (p unsafe.Pointer) (unsafe.Pointer) {
	x := uintptr (p)
	return unsafe.Pointer (x ^ 0)
}

func NoEscapeBytes (_input *[]byte) (*[]byte) {
	return (*[]byte) (NoEscape (unsafe.Pointer (_input)))
}

