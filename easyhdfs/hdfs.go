package easyhdfs

import "github.com/colinmarc/hdfs"

type HdfsFile struct {
	Path string
	*hdfs.FileReader
	*hdfs.FileWriter
	Client *hdfs.Client
}

func (f *HdfsFile) Close() error {
	if f.FileReader != nil {
		err := f.FileReader.Close()
		if err != nil {
			return err
		}
	}
	if f.FileWriter != nil {
		return f.FileWriter.Close()
	}
	return nil
}
