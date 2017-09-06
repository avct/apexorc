package apexorc

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/apex/log"
	"github.com/scritchley/orc"
)

// Test that the NumericArchiveF implementation correctly pushes log
// files back sequentially.  Foo.log becomes Foo.log.1, and should
// Foo.log.1 exist, it is pushed back to Foo.log.2, and so on, and so
// forth.
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

func TestConvertToORC(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "avct-apexorc-test-rotate-orc")
	if err != nil {
		t.Fatalf("Error from ioutil.TempDir: %s", err.Error())
	}
	//defer os.RemoveAll(tmpdir)

	path := filepath.Join(tmpdir, "testlog.orc")
	rotator, err := NewRotatingHandler(path, NumericArchiveF)
	if err != nil {
		t.Fatalf("Error creating rotating handler: %s", err)
	}
	log.SetHandler(rotator)
	log.Info("Test 1")
	log.Info("Test 2")

	// Obvisouly this locking usually happens inside rotate,
	// without it we can get intermittent failures.
	rotator.mu.Lock()
	err = rotator.handler.Close()
	if err != nil {
		t.Fatalf("Error closing journal: %s", err)
	}
	rotator.mu.Unlock()

	rotator.convertToORC(rotator.journalPath, rotator.path)
	rotatedPath := rotator.path + ".1"
	if _, err := os.Stat(rotatedPath); err != nil {
		t.Fatalf("Error converting to ORC: %s", err)
	}
	f, err := orc.Open(rotatedPath)
	if err != nil {
		t.Fatalf("Error opening ORC file: %s", err)
	}
	cursor := f.Select("message")
	if !cursor.Stripes() {
		t.Fatalf("No strips in ORC file")
	}
	if !cursor.Next() {
		t.Fatal("Cursor.Next() returned false, expected true")
	}
	row := cursor.Row()
	logMsg := "Test 1"
	msg, _ := row[0].(string)
	if msg != logMsg {
		t.Errorf("Expected %q, got %q", logMsg, msg)
	}

	if !cursor.Next() {
		t.Fatal("Cursor.Next() returned false, expected true")
	}
	row = cursor.Row()
	logMsg = "Test 2"
	msg, _ = row[0].(string)
	if msg != logMsg {
		t.Errorf("Expected %q, got %q", logMsg, msg)
	}

	f.Close()

}
