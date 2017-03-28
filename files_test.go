package easyfiles

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func CheckFileContentsMatch(f *File, contents string, expected bool, bufsize int) (bool, error) {
	var err error
	var match bool
	scanner, err := f.Reader(bufsize)
	if err != nil {
		return !expected, err
	}
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	s := ""

	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		s += scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return !expected, err
	}

	if s != contents {
		match = false
	} else {
		match = true
	}
	if match != expected {
		return false, err
	} else {
		return true, err
	}
}

func TestOpenGzFalse(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	var success bool = false
	var err error
	var f *File

	// Should pass
	f, err = Open("test/open-test.txt", os.O_RDONLY, GZ_FALSE)
	assert.Nil(err, "Failed to open valid file", err)

	success, err = CheckFileContentsMatch(f, "Hello World", true, 0)
	if err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}

	// Should succeed
	f, err = Open("test/open-test.gz", os.O_RDONLY, GZ_FALSE)
	assert.Nil(err, "Failed to open valid file", err)
	success, err = CheckFileContentsMatch(f, "Hello World", false, 0)
	if err == nil && !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}
}

func WriteGzTest(assert *assert.Assertions, bufsize int) {
	var success bool
	var err error
	var f *File
	var writer Writer

	f, err = Open(fmt.Sprintf("/tmp/write-gz-%d.gz", bufsize), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, GZ_TRUE)
	assert.Nil(err, "Failed to open valid file", err)
	defer f.Close()

	writer, err = f.Writer(bufsize)
	assert.Nil(err, "Failed to get writer to file", err)

	writer.Write([]byte("Hello World"))
	writer.Flush()
	writer.Close()
	f.Close()

	f, err = Open(f.Path, os.O_RDONLY, GZ_TRUE)
	assert.Nil(err, "Failed to open valid file", err)

	if success, err = CheckFileContentsMatch(f, "Hello World", true, bufsize); err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}
	f.Close()
	os.Remove(f.Path)
}

func TestWriteGz(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	// First do the 0 case
	WriteGzTest(assert, 0)

	wg := sync.WaitGroup{}
	wt := func(bufsize int) {
		defer wg.Done()
		WriteGzTest(assert, bufsize)
	}

	for i := 2; i < 16*1024*1024; i *= 4 {
		wg.Add(1)
		go wt(i)
	}
	wg.Wait()
}

func FlushTest(assert *assert.Assertions, bufsize int) {
	var success bool
	var err error
	var f *File

	// Should pass
	f, err = Open(fmt.Sprintf("/tmp/normal-%d.gz", bufsize), os.O_CREATE|os.O_TRUNC|os.O_RDWR, GZ_TRUE)
	assert.Nil(err, "Failed to open valid file")

	writer, err := f.Writer(bufsize)
	assert.Nil(err, "Failed to open valid file")
	writer.Write([]byte("stuff"))
	writer.Flush()
	writer.Close()

	f.Seek(0, 0)
	if success, err = CheckFileContentsMatch(f, "stuff", true, bufsize); err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}
	os.Remove(f.Path)

	// Now do it for a normal file
	f, err = Open(fmt.Sprintf("/tmp/normal-%d.txt", bufsize), os.O_CREATE|os.O_TRUNC|os.O_RDWR, GZ_FALSE)
	assert.Nil(err, "Failed to open valid file")

	writer, err = f.Writer(bufsize)
	assert.Nil(err, "Failed to open valid file")
	writer.Write([]byte("stuff"))
	writer.Flush()
	writer.Close()

	f.Seek(0, 0)
	if success, err = CheckFileContentsMatch(f, "stuff", true, bufsize); err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}
	os.Remove(f.Path)

	// Now unknown .gz
	f, err = Open(fmt.Sprintf("/tmp/unknown-%d.gz", bufsize), os.O_CREATE|os.O_TRUNC|os.O_RDWR, GZ_UNKNOWN)
	assert.Nil(err, "Failed to open valid file")

	writer, err = f.Writer(bufsize)
	assert.Nil(err, "Failed to open valid file")
	writer.Write([]byte("stuff"))
	writer.Flush()
	writer.Close()

	f.Seek(0, 0)
	if success, err = CheckFileContentsMatch(f, "stuff", true, bufsize); err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}
	os.Remove(f.Path)

	// Now unknown non-gz
	f, err = Open(fmt.Sprintf("/tmp/unknown-%d.txt", bufsize), os.O_CREATE|os.O_TRUNC|os.O_RDWR, GZ_UNKNOWN)
	assert.Nil(err, "Failed to open valid file")

	writer, err = f.Writer(bufsize)
	assert.Nil(err, "Failed to open valid file")
	writer.Write([]byte("stuff"))
	writer.Flush()
	writer.Close()

	f.Seek(0, 0)
	if success, err = CheckFileContentsMatch(f, "stuff", true, bufsize); err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}
	os.Remove(f.Path)
}

func TestFlush(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	// 0 case
	FlushTest(assert, 0)
	wg := sync.WaitGroup{}

	ft := func(bufsize int) {
		defer wg.Done()
		FlushTest(assert, bufsize)
	}

	for i := 2; i < 16*1024*1024; i *= 4 {
		wg.Add(1)
		go ft(i)
	}
	wg.Wait()
}

func TestOpenGzTrue(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	var success bool
	var err error
	var f *File

	// Should pass
	f, err = Open("test/open-test.gz", os.O_RDONLY, GZ_TRUE)
	assert.Nil(err, "Failed to open valid file")

	success, err = CheckFileContentsMatch(f, "Hello World", true, 0)
	if err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}

	// Should succeed
	f, err = Open("test/open-test.txt", os.O_RDONLY, GZ_TRUE)
	assert.Nil(err, "Failed to open valid file")

	success, err = CheckFileContentsMatch(f, "Hello World", false, 0)
	if err == nil && !success {
		assert.Fail("Should have failed to verify file contents")
	}
}

func TestOpenGzUnknown(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	var success bool
	var err error
	var f *File

	// Should pass
	f, err = Open("test/open-test.gz", os.O_RDONLY, GZ_UNKNOWN)
	assert.Nil(err, "Failed to open valid file", err)

	success, err = CheckFileContentsMatch(f, "Hello World", true, 0)
	if err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}

	// Should pass
	f, err = Open("test/open-test.txt", os.O_RDONLY, GZ_UNKNOWN)
	assert.Nil(err, "Failed to open valid file", err)

	success, err = CheckFileContentsMatch(f, "Hello World", true, 0)
	if err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}

	// Should pass
	f, err = Open("test/open-test.gz", os.O_RDONLY, GZ_UNKNOWN)
	assert.Nil(err, "Failed to open valid file", err)

	success, err = CheckFileContentsMatch(f, "Hello World", true, 0)
	if err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}

	// Should pass
	f, err = Open("test/open-gz-no-ext", os.O_RDONLY, GZ_UNKNOWN)
	assert.Nil(err, "Failed to open valid file", err)

	success, err = CheckFileContentsMatch(f, "Hello World", true, 0)
	if err != nil || !success {
		assert.Fail(fmt.Sprintf("Failed to verify file contents: %v", err))
	}

	// Should fail
	f, err = Open("test/open-test.fake.gz", os.O_RDONLY, GZ_UNKNOWN)
	assert.Nil(err, "Failed to open valid file", err)

	success, err = CheckFileContentsMatch(f, "Hello World", true, 0)
	if err == nil || success {
		assert.Fail(fmt.Sprintf("Should have failed to verify file contents: %v", err))
	}

}

func TestListFiles(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Test failure
	_, err := ListFiles("/path/that/does/not/exist", []string{"*.gz", "*.txt"})
	assert.NotNil(err, "Should have failed on non-existant path")

	answer_txt := []string{"a.txt", "b.txt"}
	answer_txt_out := []string{"a.txt.out.1", "c.txt.out.2"}
	answer_gz := []string{"a.gz", "a.sorted.gz", "c.gz"}
	answer_combined := []string{"a.txt", "a.txt.out.1", "b.txt", "c.txt.out.2"}

	patterns := [][]string{[]string{"*.txt"}, []string{"*.txt.out.*"}, []string{"*.gz"}, []string{"*.txt", "*.txt.out.*"}}
	answers := [][]string{answer_txt, answer_txt_out, answer_gz, answer_combined}

	for i := range patterns {
		p := patterns[i]
		answer := answers[i]
		files, err := ListFiles("test/list_files", p)
		assert.Nil(err, "Failed to match")

		trimmed := make([]string, len(files))
		for idx, v := range files {
			trimmed[idx] = path.Base(v)
		}
		//		fmt.Println("files:   %v", files)
		//		fmt.Println("trimmed: %v", trimmed)
		//		for idx, v := range trimmed {
		//			fmt.Println("trimmed[%v] = %v", idx, v)
		//		}

		assert.True(reflect.DeepEqual(trimmed, answer), fmt.Sprintf("Expected: %v\nGot: %v\n", answer, trimmed))
	}
}

func TestListDirs(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	answer_nr := []string{"1", "2", "3"}
	answer_r := []string{"1", "11", "111", "2", "21", "3", "31"}

	patterns := []string{"*/", "**/"}
	answers := [][]string{answer_nr, answer_r}

	for i := range patterns {
		p := patterns[i]
		answer := answers[i]
		files, _ := ListDirs("./test/testdir", []string{p})
		trimmed := make([]string, len(files))
		for idx, v := range files {
			trimmed[idx] = path.Base(v)
		}
		//		fmt.Println("files:   %v", files)
		//		fmt.Println("trimmed: %v", trimmed)
		//		for idx, v := range trimmed {
		//			fmt.Println("trimmed[%v] = %v", idx, v)
		//		}

		assert.Equal(answer, trimmed, "Did not match")
	}
}

func TestExists(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Test success
	var exists bool
	exists = Exists("./test")
	assert.Equal(true, exists, "Exists failed on existing directory")

	exists = Exists("./doesnotexist")
	assert.Equal(false, exists, "Exists failed on non-existing directory")
}

func TestMakedirs(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Give it a dir that should succeed
	base := "/tmp/longnamethatshouldntconflict"
	path := filepath.Join(base, "b/c/d/ee/ffffffffffffffffffffffffffffff/gg/hh/i/jj/k")

	err := Makedirs(path)
	assert.Nil(err, "Should have succeeded")
	os.RemoveAll(base)

	// Give it one that already exists (for coverage)
	path = "test"
	err = Makedirs(path)
	assert.Nil(err, "Should not have failed")

	// Now, one that will fail
	path = "/please/dont/allow/directory/creation/in/root"
	err = Makedirs(path)
	assert.NotNil(err, "Should have failed")
}
