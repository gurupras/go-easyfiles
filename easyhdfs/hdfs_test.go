package easyhdfs

import (
	"strings"
	"testing"

	"github.com/colinmarc/hdfs"
	"github.com/stretchr/testify/require"
)

func testClient(t *testing.T, require *require.Assertions) *hdfs.Client {
	if strings.Compare(*hdfsAddr, "") == 0 {
		t.Skip("Skipping test since no HDFS address was specified")
	}
	client, err := hdfs.New(*hdfsAddr)
	require.Nil(err)
	require.NotNil(client)
	return client
}

func TestConnect(t *testing.T) {
	require := require.New(t)
	_ = testClient(t, require)
}

func TestReadDir(t *testing.T) {
	require := require.New(t)

	client := testClient(t, require)

	files, err := client.ReadDir("/")
	require.Nil(err)
	require.NotNil(files)
	require.NotZero(len(files))
}
