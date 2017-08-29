package apexorc

import (
	"bytes"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"
	"github.com/scritchley/orc"
)

func TestNewWriter(t *testing.T) {
	buff := bytes.NewBuffer([]byte{})
	var err error
	var w *orc.Writer // Defined to enforce type check
	w, err = newWriter(buff)
	if err != nil {
		t.Fatalf("Failed in NewWriter: %s", err)
	}
	if w == nil {
		t.Fatal("Writer returned from NewWriter is nil")
	}

}

func TestWriteOneRecord(t *testing.T) {
	buff := bytes.NewBuffer([]byte{})
	w, err := newWriter(buff)
	if err != nil {
		t.Fatalf("Failed in newWriter: %s", err)
	}

	memory := memory.New()
	log.SetHandler(memory)
	log.Info("Hello")
	entry := memory.Entries[0]

	err = writeRecord(w, entry)
	if err != nil {
		t.Fatalf("Error writing record: %s", err.Error())
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Error closing orc Writer: %s", err.Error())
	}

	sizedReader := bytes.NewReader(buff.Bytes())
	r, err := orc.NewReader(sizedReader)
	if err != nil {
		t.Fatalf("Error creating orc.Reader from buffer: %s", err.Error())
	}
	cursor := r.Select("timestamp", "message", "fields")
	if !cursor.Stripes() {
		t.Fatalf("cursor.Stripes() returned false, expected true")
	}
	if !cursor.Next() {
		t.Fatalf("cursor.Next() returned false, expected true")
	}
	row := cursor.Row()
	val := row[0]
	timestamp, ok := val.(time.Time)
	if !ok {
		t.Fatal("Timestamp stored in ORC cannot be cast back to time.Time")
	}
	if entry.Timestamp.Local() != timestamp.Local() {
		t.Errorf("Expected %v, got %v", entry.Timestamp.Local(), timestamp.Local())
	}

}
