package easyhdfs

import (
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/gurupras/go-easyfiles"
	"github.com/gurupras/gocommons/gsync"
	"github.com/stretchr/testify/require"
)

var (
	hdfsAddr = flag.String("hdfs-addr", "", "Address of HDFS server")
	hdfsPath = flag.String("hdfs-path", "/test", "Base path under which serialization is tested")
)

func getHDFS(t *testing.T) easyfiles.FileSystemInterface {
	if strings.Compare(*hdfsAddr, "") == 0 {
		t.Skip(fmt.Sprintf("HDFS address not specified"))
	}

	fs := NewHDFSFileSystem(*hdfsAddr)
	return fs
}
func TestHDFSStatNonExisting(t *testing.T) {
	require := require.New(t)
	fs := getHDFS(t)

	file := "/test/test-hdfs-stat"

	info, err := fs.Stat(file)
	require.Nil(err, fmt.Sprintf("%v", err))
	require.Nil(info)
}

func TestHDFSStatExisting(t *testing.T) {
	require := require.New(t)
	fs := getHDFS(t)

	file := "/test/test-hdfs-stat-existing"
	f, err := fs.Open(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_UNKNOWN)
	require.Nil(err)
	require.NotNil(f)
	f.Close()

	info, err := fs.Stat(file)
	require.Nil(err, fmt.Sprintf("%v", err))
	require.NotNil(info)
	fs.Remove(file)
}

func TestHDFSOpenCreate(t *testing.T) {
	require := require.New(t)
	fs := getHDFS(t)

	file := "/test/hdfs-create"
	exists, err := fs.Exists(file)
	require.Nil(err, fmt.Sprintf("%v", err))
	if exists {
		err = fs.Remove(file)
		require.Nil(err)
	}
	f, err := fs.Open(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_FALSE)
	require.Nil(err)
	require.NotNil(f)
	f.Close()
	stat, err := fs.Stat(file)
	require.Nil(err)
	require.NotNil(stat)
	fs.Remove(file)

	// Now test GZ_TRUE
	f, err = fs.Open(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_TRUE)
	require.Nil(err)
	require.NotNil(f)
	f.Close()
	stat, err = fs.Stat(file)
	require.Nil(err)
	require.NotNil(stat)
	fs.Remove(file)

	// Now test GZ_UNKNOWN
	f, err = fs.Open(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_UNKNOWN)
	require.Nil(err)
	require.NotNil(f)
	f.Close()
	stat, err = fs.Stat(file)
	require.Nil(err)
	require.NotNil(stat)
	fs.Remove(file)
}

func TestManyFilesWriter(t *testing.T) {
	require := require.New(t)
	fs := getHDFS(t)

	// Generate some random data of a few MB
	size := 11 * 1024 * 1024 // 11MB

	numFiles := 20

	buf := make([]byte, size)
	rand.Read(buf)

	// Now write a bunch of files in parallel with the same data and check them at the end
	files := make([]string, 0)
	mutex := sync.Mutex{}

	sem := gsync.NewSem(8)
	wg := sync.WaitGroup{}
	for idx := 0; idx < numFiles; idx++ {
		sem.P()
		wg.Add(1)
		file := fmt.Sprintf("/test/test-hdfs-many-tiles.%08d", idx)
		go func(filename string) {
			defer sem.V()
			defer wg.Done()
			f, err := fs.Open(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_TRUE)
			require.Nil(err)
			defer f.Close()

			writer, err := f.Writer(0)
			require.Nil(err)
			defer writer.Close()
			defer writer.Flush()
			size, err := writer.Write(buf)
			require.Nil(err)
			require.Equal(len(buf), size)

			mutex.Lock()
			files = append(files, file)
			mutex.Unlock()
		}(file)
	}
	wg.Wait()
	defer func() {
		for _, file := range files {
			fs.Remove(file)
		}
	}()

	// Now all files are written. Check them
	for idx := 0; idx < numFiles; idx++ {
		sem.P()
		wg.Add(1)
		go func(idx int) {
			defer sem.V()
			defer wg.Done()
			file := files[idx]
			f, err := fs.Open(file, os.O_RDONLY, easyfiles.GZ_TRUE)
			require.Nil(err)

			got := bytes.NewBuffer(nil)
			reader, err := f.RawReader()
			require.Nil(err)
			size, err := io.Copy(got, reader)
			require.Nil(err)
			require.Equal(len(buf), int(size))
			require.Equal(buf, got.Bytes(), fmt.Sprintf("File did not match: %v", file))
		}(idx)
	}
	wg.Wait()
}
