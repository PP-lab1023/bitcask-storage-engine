package benchmark

import (
	kvproject "bitcask-go"
	"bitcask-go/utils"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var db *kvproject.DB

func init() {
	// Initialize database for http
	var err error
	options := kvproject.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.DirPath = dir
	db, err = kvproject.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func Benchmark_Put(b *testing.B) {
	// The timer needs to be reset before officially starting the test
	b.ResetTimer()
	// Print the memory allocation of each method during the test run
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Change the parameter to RandomValue()(ie. size(data)) to get impact of data size on performance
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Randomly get
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != kvproject.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}