package easyfiles

import (
	"io/ioutil"
	"os"
)

type localFileSystem struct {
}

var LocalFS = localFileSystem{}

func (l localFileSystem) Open(path string, mode int, gz FileType) (*File, error) {
	return Open(path, mode, gz)
}

func (l localFileSystem) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

func (l localFileSystem) ReadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func (l localFileSystem) WriteFile(path string, b []byte, perm os.FileMode) error {
	return ioutil.WriteFile(path, b, perm)
}
