package apexorc

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/apex/log"
)

func TestNumericArchiveF(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "avct-apexorc-test-rotate")
	if err != nil {
		t.Fatalf("Error from ioutil.TempDir: %s", err.Error())
	}
	defer os.RemoveAll(tmpdir)
	path := filepath.Join(tmpdir, "testlog.orc")

	original := []byte{'t', 'e', 's', 't'}
	err = ioutil.WriteFile(path, original, 0600)
	if err != nil {
		t.Fatalf("Error creating tempfile: %s", err.Error())
	}

	err = NumericArchiveF(path)
	if err != nil {
		t.Fatalf("Error archiving: %s", err.Error())
	}
	expectedPath := path + ".1"
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Fatalf("Expected file rotated to %q, but it doesn't exist", expectedPath)
	}
	content, err := ioutil.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Error reading rotated file: %q", err.Error())
	}
	if string(content) != string(original) {
		t.Errorf("Expected the rotated file to contain the original content, %q, but found %q", original, content)
	}

	// OK, if we now create and rotate a 2nd file then the
	// original one should get rotated to .2
	other := []byte{'o', 't', 'h', 'e', 'r'}
	err = ioutil.WriteFile(path, other, 0600)
	if err != nil {
		t.Fatalf("Error creating tempfile: %s", err.Error())
	}
	err = NumericArchiveF(path)
	if err != nil {
		t.Fatalf("Error archiving: %s", err.Error())
	}
	// The new file should now be .1
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Fatalf("Expected file rotated to %q, but it doesn't exist", expectedPath)
	}
	content, err = ioutil.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Error reading rotated file: %q", err.Error())
	}
	if string(content) != string(other) {
		t.Errorf("Expected the 1st rotated file to contain the content, %q, but found %q", other, content)
	}

	// And there should now be a .2 file, with the original content
	expectedPath = path + ".2"
	content, err = ioutil.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Error reading rotated file: %q", err.Error())
	}
	if string(content) != string(original) {
		t.Errorf("Expected the 2nd rotated file to contain the content, %q, but found %q", original, content)
	}

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
	rotator := NewRotatingHandler(path, NumericArchiveF)
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
