package easyfiles

import (
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Copy of ioutil/tempfile_test.go

func TestTempFile(t *testing.T) {
	t.Parallel()

	f, err := TempFile("/_not_exists_", "foo")
	if f != nil || err == nil {
		t.Errorf("TempFile(`/_not_exists_`, `foo`) = %v, %v", f, err)
	}

	dir := os.TempDir()
	f, err = TempFile(dir, "ioutil_test")
	if f == nil || err != nil {
		t.Errorf("TempFile(dir, `ioutil_test`) = %v, %v", f, err)
	}
	if f != nil {
		f.Close()
		os.Remove(f.Name())
		re := regexp.MustCompile("^" + regexp.QuoteMeta(filepath.Join(dir, "ioutil_test")) + "[0-9]+$")
		if !re.MatchString(f.Name()) {
			t.Errorf("TempFile(`"+dir+"`, `ioutil_test`) created bad name %s", f.Name())
		}
	}
}

func TestTempFileWithSuffix(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	f, err := TempFile("", "test", ".gz")
	assert.Nil(err, "Failed to create tempfile with valid suffix")
	assert.NotNil(f, "Failed to create tempfile with valid suffix")

	f, err = TempFile("", "test", ".gz", ".ggz")
	assert.NotNil(err, "Should have failed with multiple suffixes")
	assert.Nil(f, "Should have failed with multiple suffixes")
}

func TestLargeTempFile(t *testing.T) {
	t.Parallel()

	var err error
	assert := assert.New(t)

	dir := os.TempDir()
	f := filepath.Join(dir, "ioutil_test")
	err = Makedirs(f)
	assert.Nil(err, "Failed to create directory")
	var tmpFile *os.File
	tmpFiles := make([]string, 0)

	wg := sync.WaitGroup{}

	fn := func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			tmpFile, err = TempFile(f, "temp-", ".test")
			tmpFile.Close()
			assert.Nil(err, "Failed to create temp file")
			tmpFiles = append(tmpFiles, tmpFile.Name())
		}
		for _, tmp := range tmpFiles {
			os.Remove(tmp)
		}
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go fn()
	}
	wg.Wait()
	os.RemoveAll(f)

}
