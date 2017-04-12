package easyfiles

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bmatcuk/doublestar"
)

type FileType int

const (
	GZ_TRUE    FileType = 1
	GZ_FALSE   FileType = 0
	GZ_UNKNOWN FileType = -1
)

const (
	DEFAULT_BUFSIZE = 4 * 1024 * 1024
)

type FileInterface interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

type File struct {
	Path string
	File FileInterface
	Mode int
	Gz   FileType
}

type IWriter interface {
	Write(bytes []byte) (int, error)
	Reset(w io.Writer)
	Flush() error
	//Close() error
}

type Writer struct {
	IWriter
	gz FileType
}

func (f *File) fixMode() {
	// First, the simple case
	if strings.HasSuffix(f.Path, ".gz") {
		f.Gz = GZ_TRUE
	} else {
		// Remember, all of this only occurs when gz is set to GZ_UNKNOWN
		// So if a file is in write mode, has a non .gz suffix and is
		// set to GZ_UNKNOWN, we're obviously going to give back a regular
		// non-gz file
		f.Gz = GZ_FALSE

		// Try to get a reader to figure it out
		if f.Mode|os.O_RDONLY|os.O_RDWR != 0 {
			// We have read privilege..try to get a gzip reader
			reader, err := gzip.NewReader(f.File)
			if err == nil {
				f.Gz = GZ_TRUE
				defer reader.Close()
			} else {
				f.Gz = GZ_FALSE
			}
		}
		// We can freely seek at this point
		// This occurs on Open at which point the user is just
		// opening the file and cannot do any operation on it.
		// So, we can seek back and return as Open always does
		// - at the start of the file
		f.File.Seek(0, os.SEEK_SET)
	}
}

func (w *Writer) Flush() (err error) {
	if w.gz == GZ_TRUE {
		if v, ok := w.IWriter.(*gzip.Writer); ok {
			return v.Flush()
		}
	}
	v, _ := w.IWriter.(*bufio.Writer)
	err = v.Flush()
	return
}

func (w *Writer) Close() (err error) {
	if w.gz == GZ_TRUE {
		if v, ok := w.IWriter.(*gzip.Writer); ok {
			err = v.Close()
		}
	}
	return
}

func (f *File) RawReader() (io.Reader, error) {
	gz_open := false
	var reader io.Reader
	var err error

	switch f.Gz {
	case GZ_TRUE:
		gz_open = true
	case GZ_FALSE:
		// Nothing to do
	case GZ_UNKNOWN:
		panic("Should not have occured..mode should have been fixed on open")
	}

	if gz_open == true {
		reader, err = gzip.NewReader(f.File)
	} else {
		reader = bufio.NewReader(f.File)
	}
	return reader, err
}

func (f *File) Reader(bufsize int) (*bufio.Scanner, error) {
	reader, err := f.RawReader()
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(reader)

	var buf []byte
	if bufsize != 0 {
		buf = make([]byte, 0, bufsize)
	} else {
		buf = make([]byte, 0, DEFAULT_BUFSIZE)
	}

	scanner.Buffer(buf, bufsize)
	return scanner, err
}

func (f *File) Writer(bufsize int) (Writer, error) {
	gz_open := false
	var writer IWriter
	var err error

	switch f.Gz {
	case GZ_TRUE:
		gz_open = true
	case GZ_FALSE:
		// Nothing to do
	default:
		panic("Should not have occured..mode should have been fixed on open")
	}

	if gz_open == true {
		writer = gzip.NewWriter(f.File)
	} else {
		writer = bufio.NewWriter(f.File)
	}

	if bufsize != 0 {
		// FIXME: Figure out why we're unable to wrap a gzipWriter with
		// a bufio writer
	}
	return Writer{writer, f.Gz}, err
}

func (f *File) Close() {
	f.File.Close()
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.File.Seek(offset, whence)
}

func Open(filepath string, mode int, gz FileType) (*File, error) {
	var retfile *File
	var err error

	file, err := os.OpenFile(filepath, mode, 0664)
	if err == nil {
		retfile = &File{filepath, file, mode, gz}
		if gz == GZ_UNKNOWN {
			retfile.fixMode()
		}
	}
	return retfile, err
}

func ListFiles(fpath string, patterns []string) (matches []string, err error) {
	_, err = os.Stat(fpath)
	if err != nil {
		return nil, err
	}

	visit := func(fp string, fi os.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return nil
		}
		if fi.IsDir() {
			return nil
		}
		var matched bool
		for _, pattern := range patterns {
			var m bool
			m, err = filepath.Match(pattern, fi.Name())
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return err
			}
			matched = m || matched
		}
		if matched {
			matches = append(matches, fp)
		}
		return nil
	}
	filepath.Walk(fpath, visit)
	sort.Strings(matches)
	return
}

func IsDir(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	return fileInfo.IsDir(), err
}

func ListDirs(fpath string, patterns []string) (matches []string, err error) {
	var dirs []string
	abs, _ := filepath.Abs(fpath)
	for _, pattern := range patterns {
		globPattern := abs + "/" + pattern
		if dirs, err = doublestar.Glob(globPattern); err != nil {
			err = errors.New(fmt.Sprintf("Failed to glob: %v", err))
			return
		}
		for _, d := range dirs {
			if ok, _ := IsDir(d); ok {
				matches = append(matches, d)
			}
		}
	}
	sort.Strings(matches)
	return
}

func Exists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else {
		return os.IsExist(err)
	}
}

func Makedirs(path string) error {
	if exist := Exists(path); !exist {
		return os.MkdirAll(path, 0775)
	}
	return nil
}
