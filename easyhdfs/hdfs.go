package easyhdfs

import "github.com/colinmarc/hdfs"

type HdfsFile struct {
	Path string
	*hdfs.FileReader
	Writer *hdfs.FileWriter
	Client *hdfs.Client
}

func (f *HdfsFile) Write(b []byte) (int, error) {
	return f.Writer.Write(b)
}

func (f *HdfsFile) Close() error {
	if f.FileReader != nil {
		err := f.FileReader.Close()
		if err != nil {
			return err
		}
	}
	if f.Writer != nil {
		return f.Writer.Close()
	}
	return nil
}
