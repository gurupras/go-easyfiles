package easyfiles

import (
	"io/ioutil"
	"os"

	"github.com/bmatcuk/doublestar"
)

type localFileSystem struct {
}

var LocalFS = localFileSystem{}

func (l localFileSystem) Open(name string, mode int, gz FileType) (*File, error) {
	return Open(name, mode, gz)
}

func (l localFileSystem) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (l localFileSystem) ReadFile(name string) ([]byte, error) {
	return ioutil.ReadFile(name)
}

func (l localFileSystem) WriteFile(name string, b []byte, perm os.FileMode) error {
	return ioutil.WriteFile(name, b, perm)
}

func (l localFileSystem) Remove(name string) error {
	return os.Remove(name)
}

func (l localFileSystem) RemoveAll(name string) error {
	return os.RemoveAll(name)
}

func (l localFileSystem) Makedirs(name string) error {
	return Makedirs(name)
}

func (l localFileSystem) Exists(name string) (bool, error) {
	return Exists(name), nil
}

func (l localFileSystem) Glob(pattern string) ([]string, error) {
	return doublestar.Glob(pattern)
}

func (l localFileSystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}
