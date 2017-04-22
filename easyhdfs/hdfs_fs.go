package easyhdfs

import (
	"fmt"
	"os"
	"sync"

	"github.com/colinmarc/hdfs"
	"github.com/fatih/set"
	"github.com/gurupras/go-easyfiles"
	"github.com/gurupras/go-hdfs-doublestar"
	"github.com/gurupras/gocommons/gsync"
	log "github.com/sirupsen/logrus"
)

const DEFAULT_POOL_SIZE = 16

type Pool struct {
	sync.Mutex
	size int
	*gsync.Semaphore
	pool set.Interface
}

func NewPool(elements ...interface{}) *Pool {
	p := &Pool{}
	p.size = len(elements)
	p.pool = set.New()
	p.Semaphore = gsync.NewSem(p.size)
	p.pool.Add(elements...)
	return p
}

func (p *Pool) get() interface{} {
	p.P()
	p.Lock()
	defer p.Unlock()
	return p.pool.Pop()
}

func (p *Pool) put(obj interface{}) {
	p.Lock()
	defer p.Unlock()
	p.pool.Add(obj)
	p.V()
}

type HDFSFileSystem struct {
	Addr       string
	clientPool *Pool
}

func NewHDFSFileSystem(addr string, poolSize ...int) *HDFSFileSystem {
	fs := &HDFSFileSystem{addr, nil}
	size := DEFAULT_POOL_SIZE
	if len(poolSize) > 0 && poolSize[0] > 1 {
		size = poolSize[0]
	}
	log.Infof("Creating pool of size: %v", size)
	clients := make([]interface{}, size)
	for idx := 0; idx < size; idx++ {
		client, err := hdfs.New(addr)
		if err != nil {
			log.Errorf("Failed to connect to HDFS at address '%v': %v", addr, err)
			return nil
		}
		clients[idx] = client
	}
	pool := NewPool(clients...)
	fs.clientPool = pool
	return fs
}

func (h *HDFSFileSystem) getClient() (*hdfs.Client, error) {
	client := h.clientPool.get().(*hdfs.Client)
	return client, nil
}

func (h *HDFSFileSystem) putClient(obj interface{}) {
	h.clientPool.put(obj)
}

func (h *HDFSFileSystem) Open(path string, mode int, gz easyfiles.FileType) (*easyfiles.File, error) {
	client, err := h.getClient()
	if err != nil {
		return nil, err
	}

	hFile := &hdfsFile{path, nil, nil, client, h}
	// Check if file exists
	// If a file does not exist, this throws an error
	stat, err := client.Stat(path)
	_ = stat
	truncCreate := false
	if err == nil {
		if stat == nil {
			// That's weird ...
			log.Fatalf("stat and err are nil for file: %v", path)
		}
		// We have a file that exists
		// Do we need to truncate it?
		log.Debugf("stat is not nil.. File exists. Check if we need to truncate: %v", path)
		if mode&os.O_TRUNC > 0 || (mode&os.O_WRONLY > 0 && mode&os.O_APPEND == 0) {
			log.Debugf("Truncating file: %v", path)
			// If O_TRUNC is set, then truncate
			// Otherwise, if in write mode and
			// no O_APPEND is set, truncate
			err = client.Remove(path)
			if err != nil {
				return nil, fmt.Errorf("%v", err)
			}
			truncCreate = true
		}
	}
	if err != nil || truncCreate {
		// File does not exist
		// Check if we have to create it
		log.Debugf("Check if we have to create file: %v", path)
		if mode&os.O_CREATE > 0 || truncCreate {
			log.Debugf("Creating empty file: %v", path)
			if err := client.CreateEmptyFile(path); err != nil {
				return nil, err
			}
		}
	}
	// At this point, we're sure the file exists
	// Get reader to file
	log.Debugf("Opening file with read: %v", path)
	f, err := client.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	hFile.FileReader = f

	if mode&os.O_WRONLY > 0 || mode&os.O_RDWR > 0 {
		// We don't need to check for O_APPEND, because that's
		// the only mode we support
		log.Debugf("Opening file with write: %v", path)
		w, err := client.Append(path)
		if err != nil {
			return nil, fmt.Errorf("%v", err)
		}
		hFile.Writer = w
	}
	file := &easyfiles.File{path, hFile, mode, gz}
	// Now make sure you fix GZ_UNKNOWN if it is GZ_UNKNOWN
	file.FixMode()
	return file, nil
}

func (h *HDFSFileSystem) Stat(name string) (os.FileInfo, error) {
	client, err := h.getClient()
	if err != nil {
		return nil, err
	}
	defer h.putClient(client)

	info, err := client.Stat(name)
	if err != nil {
		return nil, nil
	} else if info == nil {
		return nil, nil
	} else {
		return info, nil
	}
}

func (h *HDFSFileSystem) ReadFile(name string) ([]byte, error) {
	client, err := h.getClient()
	if err != nil {
		return nil, err
	}
	defer h.putClient(client)

	return client.ReadFile(name)
}

func (h *HDFSFileSystem) WriteFile(name string, b []byte, perm os.FileMode) error {
	f, err := h.Open(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_FALSE)
	if err != nil {
		return err
	}
	defer f.Close()

	writer, err := f.Writer(0)
	if err != nil {
		return err
	}
	defer writer.Close()
	defer writer.Flush()

	if _, err = writer.Write(b); err != nil {
		return err
	}
	return nil
}

func (h *HDFSFileSystem) Remove(name string) error {
	client, err := h.getClient()
	if err != nil {
		return err
	}
	defer h.putClient(client)
	return client.Remove(name)
}

func (h *HDFSFileSystem) RemoveAll(name string) error {
	return h.Remove(name)
}

func (h *HDFSFileSystem) Makedirs(name string) error {
	client, err := h.getClient()
	if err != nil {
		return err
	}
	defer h.putClient(client)
	return client.MkdirAll(name, 0775)
}

func (h *HDFSFileSystem) Exists(name string) (bool, error) {
	info, err := h.Stat(name)
	if err != nil {
		return false, err
	} else if info == nil {
		return false, nil
	} else {
		return true, nil
	}
}

func (h *HDFSFileSystem) Glob(pattern string) ([]string, error) {
	client, err := h.getClient()
	if err != nil {
		return nil, err
	}
	defer h.putClient(client)
	return hdfs_doublestar.Glob(client, pattern)
}
