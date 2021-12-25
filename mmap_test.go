
package cdb_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/colinmarc/cdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


func TestGetMmap(t *testing.T) {
	db, err := cdb.OpenMmap("./test/test.cdb")
	require.NoError(t, err)
	require.NotNil(t, db)

	records := append(append(expectedRecords, expectedRecords...), expectedRecords...)
	shuffle(records)

	for _, record := range records {
		msg := "while fetching " + string(record[0])

		value, err := db.Get(record[0])
		require.NoError(t, err, msg)
		assert.Equal(t, string(record[1]), string(value), msg)
	}
}


func BenchmarkGetMmap(b *testing.B) {
	db, _ := cdb.OpenMmap("./test/test.cdb")
	b.ResetTimer()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		record := expectedRecords[rand.Intn(len(expectedRecords))]
		db.Get(record[0])
	}
}

