package easyhdfs

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gurupras/go-easyfiles"
	"github.com/stretchr/testify/require"
)

var (
	hdfsAddr = flag.String("hdfs-addr", "", "Address of HDFS server")
	hdfsPath = flag.String("hdfs-path", "/test", "Base path under which serialization is tested")
)

func TestHDFSStatNonExisting(t *testing.T) {
	if strings.Compare(*hdfsAddr, "") == 0 {
		t.Skip(fmt.Sprintf("HDFS address not specified"))
	}

	fs := NewHDFSFileSystem(*hdfsAddr)

	require := require.New(t)

	file := "/test/test-hdfs-stat"

	info, err := fs.Stat(file)
	require.Nil(err, fmt.Sprintf("%v", err))
	require.Nil(info)
}

func TestHDFSStatExisting(t *testing.T) {
	if strings.Compare(*hdfsAddr, "") == 0 {
		t.Skip(fmt.Sprintf("HDFS address not specified"))
	}

	fs := NewHDFSFileSystem(*hdfsAddr)

	require := require.New(t)

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
	if strings.Compare(*hdfsAddr, "") == 0 {
		t.Skip(fmt.Sprintf("HDFS address not specified"))
	}

	fs := NewHDFSFileSystem(*hdfsAddr)

	require := require.New(t)

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
	fs.Remove(file)

	// Now test GZ_TRUE
	f, err = fs.Open(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_TRUE)
	require.Nil(err)
	require.NotNil(f)
	f.Close()
	fs.Remove(file)

	// Now test GZ_UNKNOWN
	f, err = fs.Open(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_UNKNOWN)
	require.Nil(err)
	require.NotNil(f)
	f.Close()
	fs.Remove(file)
}
