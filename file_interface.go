package easyfiles

import "os"

type FileSystemInterface interface {
	Open(string, int, FileType) (*File, error)
	Stat(string) (os.FileInfo, error)
	ReadFile(string) ([]byte, error)
	WriteFile(string, []byte, os.FileMode) error
}
