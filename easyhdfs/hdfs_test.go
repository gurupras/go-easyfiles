package easyhdfs

import (
	"testing"

	"github.com/colinmarc/hdfs"
	"github.com/stretchr/testify/require"
)

func testClient(require *require.Assertions) *hdfs.Client {
	client, err := hdfs.New("dirtydeeds.cse.buffalo.edu:9000")
	require.Nil(err)
	require.NotNil(client)
	return client
}

func TestConnect(t *testing.T) {
	require := require.New(t)
	_ = testClient(require)
}

func TestReadDir(t *testing.T) {
	require := require.New(t)

	client := testClient(require)

	files, err := client.ReadDir("/")
	require.Nil(err)
	require.NotNil(files)
	require.NotZero(len(files))
}

func TestRecursiveList(t *testing.T) {
	require := require.New(t)

	client := testClient(require)

	files, err := RecursiveList(client, "/", "i*json")
	require.Nil(err)
	require.NotZero(len(files))
}
