package cdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	assert.EqualValues(t, 776976811, CDBHash([]byte("foo bar baz")))
	assert.EqualValues(t, 3538394712, CDBHash([]byte("The quick brown fox jumped over the lazy dog")))
}
