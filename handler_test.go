package apexorc

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/apex/log"
	"github.com/scritchley/orc"
)

func TestHandleLog(t *testing.T) {
	buff := bytes.NewBuffer([]byte{})
	w, err := newWriter(buff)
	if err != nil {
		t.Fatalf("Failed in newWriter: %s", err)
	}

	// The path here doesn't matter as we're going to inject a writer
	handler := NewHandler("Path")
	handler.writer = w

	log.SetHandler(handler)

	logMsg := "Violin Bow Exception"
	testError := errors.New("Fiddlesticks! It's broken")
	log.WithError(testError).Error(logMsg)

	err = handler.Close()
	if err != nil {
		t.Fatalf("Error closing handler: %q", err.Error())
	}
	sizedReader := bytes.NewReader(buff.Bytes())
	r, err := orc.NewReader(sizedReader)
	if err != nil {
		t.Fatalf("Error creating orc.Reader from buffer: %s", err.Error())
	}
	td := r.Schema()
	columns := td.Columns()
	expectedColumns := []string{"timestamp", "level", "message", "fields"}
	if !reflect.DeepEqual(expectedColumns, columns) {
		t.Fatalf("Expected columns %q, got %q", expectedColumns, columns)
	}
	cursor := r.Select("message")
	if !cursor.Stripes() {
		t.Fatal("Cursor.Stripes() returned false, expected true")
	}
	if !cursor.Next() {
		t.Fatal("Cursor.Next() returned false, expected true")
	}
	row := cursor.Row()
	// We'll just check one value as the full spectrum testing is handled in writer_test.go
	msg, _ := row[0].(string)
	if msg != logMsg {
		t.Errorf("Expected %q, got %q", logMsg, msg)
	}

}
