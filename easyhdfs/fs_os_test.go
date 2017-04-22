package easyhdfs

import (
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/gurupras/go-easyfiles"
)

// These tests are copied from os_test.go

type openErrorTest struct {
	path  string
	mode  int
	error error
}

var sfdir = "/etc"
var sfname = "/etc/groups"

var openErrorTests = []openErrorTest{
	{
		sfdir + "/no-such-file",
		os.O_RDONLY,
		syscall.ENOENT,
	},
	{
		sfdir,
		os.O_WRONLY,
		syscall.EISDIR,
	},
	{
		sfdir + "/" + sfname + "/no-such-file",
		os.O_WRONLY,
		syscall.ENOTDIR,
	},
}

func testOpenError(t *testing.T, fs easyfiles.FileSystemInterface) {
	for _, tt := range openErrorTests {
		f, err := fs.Open(tt.path, tt.mode, easyfiles.GZ_FALSE)
		if err == nil {
			t.Errorf("Open(%q, %d) succeeded", tt.path, tt.mode)
			f.Close()
			continue
		}
	}
}

func testOpenNoName(t *testing.T, fs easyfiles.FileSystemInterface) {
	f, err := fs.Open("", os.O_RDONLY, easyfiles.GZ_FALSE)
	if err == nil {
		t.Fatal(`Open("") succeeded`)
		f.Close()
	}
}

func newFile(name string, fs easyfiles.FileSystemInterface, t *testing.T) *easyfiles.File {
	f, err := fs.Open(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_FALSE)
	if err != nil {
		t.Fatalf("Failed to create new file %v: %v", name, err)
	}
	f.Close()
	return f
}

/*
func testReadAt(t *testing.T, fs easyfiles.FileSystemInterface) {
	f := newFile("TestReadAt", t)
	defer fs.Remove(f.Name())
	defer f.Close()
	const data = "hello, world\n"
	io.WriteString(f.File, data)
	b := make([]byte, 5)
	n, err := f.ReadAt(b, 7)
	if err != nil || n != len(b) {
		t.Fatalf("ReadAt 7: %d, %v", n, err)
	}
	if string(b) != "world" {
		t.Fatalf("ReadAt 7: have %q want %q", string(b), "world")
	}
}

// Verify that ReadAt doesn't affect seek offset.
func testReadAtOffset(t *testing.T, fs easyfiles.FileSystemInterface) {
	f := newFile("TestReadAtOffset", t)
	defer Remove(f.Name())
	defer f.Close()
	const data = "hello, world\n"
	io.WriteString(f, data)
	f.Seek(0, 0)
	b := make([]byte, 5)
	n, err := f.ReadAt(b, 7)
	if err != nil || n != len(b) {
		t.Fatalf("ReadAt 7: %d, %v", n, err)
	}
	if string(b) != "world" {
		t.Fatalf("ReadAt 7: have %q want %q", string(b), "world")
	}
	n, err = f.Read(b)
	if err != nil || n != len(b) {
		t.Fatalf("Read: %d, %v", n, err)
	}
	if string(b) != "hello" {
		t.Fatalf("Read: have %q want %q", string(b), "hello")
	}
}

func testWriteAt(t *testing.T, fs easyfiles.FileSystemInterface) {
	f := newFile("TestWriteAt", t)
	defer Remove(f.Name())
	defer f.Close()
	const data = "hello, world\n"
	io.WriteString(f, data)
	n, err := f.WriteAt([]byte("WORLD"), 7)
	if err != nil || n != 5 {
		t.Fatalf("WriteAt 7: %d, %v", n, err)
	}
	b, err := ioutil.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("ReadFile %s: %v", f.Name(), err)
	}
	if string(b) != "hello, WORLD\n" {
		t.Fatalf("after write: have %q want %q", string(b), "hello, WORLD\n")
	}
}
*/

func writeFile(t *testing.T, fs easyfiles.FileSystemInterface, fname string, flag int, text string) string {
	f, err := fs.Open(fname, flag, easyfiles.GZ_FALSE)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	n, err := io.WriteString(f.File, text)
	if err != nil {
		t.Fatalf("WriteString: %d, %v", n, err)
	}
	f.Close()
	data, err := fs.ReadFile(fname)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return string(data)
}

func testAppend(t *testing.T, fs easyfiles.FileSystemInterface) {
	const f = "/tmp/append.txt"
	defer fs.Remove(f)
	s := writeFile(t, fs, f, os.O_CREATE|os.O_TRUNC|os.O_RDWR, "new")
	if s != "new" {
		t.Fatalf("writeFile: have %q want %q", s, "new")
	}
	s = writeFile(t, fs, f, os.O_APPEND|os.O_RDWR, "|append")
	if s != "new|append" {
		t.Fatalf("writeFile: have %q want %q", s, "new|append")
	}
	s = writeFile(t, fs, f, os.O_CREATE|os.O_APPEND|os.O_RDWR, "|append")
	if s != "new|append|append" {
		t.Fatalf("writeFile: have %q want %q", s, "new|append|append")
	}
	err := fs.Remove(f)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	s = writeFile(t, fs, f, os.O_CREATE|os.O_APPEND|os.O_RDWR, "new&append")
	if s != "new&append" {
		t.Fatalf("writeFile: after append have %q want %q", s, "new&append")
	}
	// XXX: This test will 100% fail on HDFS since HDFS supports only append at end
	/*
		s = writeFile(t, fs, f, os.O_CREATE|os.O_RDWR, "old")
		if s != "old&append" {
			t.Fatalf("writeFile: after create have %q want %q", s, "old&append")
		}
	*/
	s = writeFile(t, fs, f, os.O_CREATE|os.O_TRUNC|os.O_RDWR, "new")
	if s != "new" {
		t.Fatalf("writeFile: after truncate have %q want %q", s, "new")
	}
}

func TestHDFSOpen(t *testing.T) {
	if strings.Compare(*hdfsAddr, "") == 0 {
		t.Skip(fmt.Sprintf("HDFS address not specified"))
	}

	fs := NewHDFSFileSystem(*hdfsAddr)
	testOpenError(t, fs)
	testOpenNoName(t, fs)
}

func TestLocalOpen(t *testing.T) {
	testOpenError(t, easyfiles.LocalFS)
	testOpenNoName(t, easyfiles.LocalFS)
}

func TestHDFSAppend(t *testing.T) {
	if strings.Compare(*hdfsAddr, "") == 0 {
		t.Skip(fmt.Sprintf("HDFS address not specified"))
	}

	fs := NewHDFSFileSystem(*hdfsAddr)
	testAppend(t, fs)
}

func TestLocalAppend(t *testing.T) {
	testAppend(t, easyfiles.LocalFS)
}
