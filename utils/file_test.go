package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(filepath.Join(dir))
	assert.Nil(t, err)
	t.Log(dirSize)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	t.Log(size)
	assert.True(t, size > 0)
}