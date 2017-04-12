package easyhdfs

import (
	"path/filepath"
	"regexp"

	"github.com/colinmarc/hdfs"
)

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

func RecursiveList(client *hdfs.Client, path, pattern string) ([]string, error) {
	result := make([]string, 0)
	regex := regexp.MustCompile(pattern)
	err := recursiveList(client, path, regex, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func recursiveList(client *hdfs.Client, path string, pattern *regexp.Regexp, result *[]string) error {
	stat, err := client.Stat(path)
	if err != nil {
		return err
	}
	if stat.IsDir() {
		entries, err := client.ReadDir(path)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			fullPath := filepath.Join(path, entry.Name())
			//fmt.Printf("%v\n", fullPath)
			if entry.IsDir() {
				err := recursiveList(client, fullPath, pattern, result)
				if err != nil {
					return err
				}
			} else {
				if pattern.Match([]byte(entry.Name())) {
					*result = append(*result, fullPath)
				}
			}
		}
	}
	return nil
}
