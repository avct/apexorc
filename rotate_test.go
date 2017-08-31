package apexorc

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/apex/log"
)

func messageInORCFile(message string, path string) bool {
	return true
}

func testArchiveF(oldPath string) string {
	dir, fileName := path.Split(oldPath)
	extension := path.Ext(fileName)
	prefix := fileName[:len(fileName)-len(extension)]
	fmt.Printf("Dir: %q, Ext: %q, prefix %q", dir, extension, prefix)
	return prefix
}

// The RotatingHandler passes through HandleLog requests to a
// subordinate Handler instance.
func TestRotatingHandler(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "avct-apexorc-test-rotate")
	if err != nil {
		t.Fatalf("Error from ioutil.TempDir: %s", err.Error())
	}
	defer os.RemoveAll(tmpdir)
	path := filepath.Join(tmpdir, "testlog.orc")
	rotator := NewRotatingHandler(path, testArchiveF)
	log.SetHandler(rotator)
	log.Info("Test 1")
	err = rotator.Rotate()
	if err != nil {
		t.Fatalf("Error in rotation: %q", err.Error())
	}
	log.Info("Test 2")
	err = rotator.Rotate()
	if err != nil {
		t.Fatalf("Error in rotation: %q", err.Error())
	}
}
