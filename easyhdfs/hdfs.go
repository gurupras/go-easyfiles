package easyhdfs

import (
	"github.com/colinmarc/hdfs"
	"github.com/labstack/gommon/log"
)

type hdfsFile struct {
	Path string
	*hdfs.FileReader
	Writer *hdfs.FileWriter
	client *hdfs.Client
	*HDFSFileSystem
}

func (f *hdfsFile) Write(b []byte) (int, error) {
	//log.Infof("Wrote %v bytes to %v", len(b), f.Name())
	return f.Writer.Write(b)
}

func (f *hdfsFile) Close() error {
	if f.FileReader != nil {
		err := f.FileReader.Close()
		if err != nil {
			log.Warnf("Failed to close reader: %v", err)
			return err
		}
	}
	if f.Writer != nil {
		return f.Writer.Close()
	}
	// Release this client
	f.HDFSFileSystem.putClient(f.client)

	return nil
}
