package fio

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	// Empty
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	t.Log(n1)
	assert.Equal(t, n1, 0)
	t.Log(err)
	assert.Equal(t, err, io.EOF)

	// Need some data to test MMap, but MMap cannot be used to write
	// So use fileIO to write some data
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size , err := mmapIO2.Size()
	assert.Equal(t, size, int64(6))
	assert.Nil(t, err)

	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	t.Log(n2)
	assert.Equal(t, n2, 2)
	t.Log(err)
	assert.Nil(t, err)
	t.Log(string(b2))
	assert.Equal(t, string(b2), "aa")
}