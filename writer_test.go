package apexorc

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"
	"github.com/scritchley/orc"
)

func makeTestEntry(msg string, fields log.Fielder, err error) *log.Entry {
	mem := memory.New()
	log.SetHandler(mem)
	logger := log.Log
	if fields != nil {
		logger = logger.WithFields(fields)
	}
	if err != nil {
		logger = logger.WithError(err)
	}
	logger.Info(msg)
	return mem.Entries[0]
}

func testWriteRecordToORCBuffer(t *testing.T, entry *log.Entry, caseN int) *bytes.Buffer {
	buff := bytes.NewBuffer([]byte{})
	w, err := newWriter(buff)
	if err != nil {
		t.Fatalf("[Case: %d] Failed in newWriter: %s", caseN, err)
	}
	err = writeRecord(w, entry)
	if err != nil {
		t.Fatalf("[Case: %d] Error writing record: %s", caseN, err.Error())
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("[Case: %d] Error closing orc Writer: %s", caseN, err.Error())
	}
	return buff
}

func testReadRowFromORCBuffer(t *testing.T, buff *bytes.Buffer, src *log.Entry, caseN int) {
	sizedReader := bytes.NewReader(buff.Bytes())
	r, err := orc.NewReader(sizedReader)
	if err != nil {
		t.Fatalf("[Case: %d] Error creating orc.Reader from buffer: %s", caseN, err.Error())
	}
	cursor := r.Select("timestamp", "level", "message", "fields")
	if !cursor.Stripes() {
		t.Fatalf("[Case: %d] cursor.Stripes() returned false, expected true", caseN)
	}
	if !cursor.Next() {
		t.Fatalf("[Case: %d] cursor.Next() returned false, expected true", caseN)
	}
	row := cursor.Row()
	val := row[0]
	timestamp, ok := val.(time.Time)
	if !ok {
		t.Fatalf("[Case: %d] Timestamp stored in ORC cannot be cast back to time.Time", caseN)
	}
	if src.Timestamp.Local() != timestamp.Local() {
		t.Errorf("[Case: %d] Expected %v, got %v", caseN, src.Timestamp.Local(), timestamp.Local())
	}

	val = row[1]
	level, ok := val.(string)
	if !ok {
		t.Fatalf("[Case: %d] Message stored in ORC cannot be cast to string", caseN)
	}
	if src.Level.String() != level {
		t.Errorf("[Case: %d] Expected %q, got %q", caseN, src.Level, level)
	}

	val = row[2]
	message, ok := val.(string)
	if !ok {
		t.Fatalf("[Case: %d] Message stored in ORC cannot be cast back to string", caseN)
	}
	if src.Message != message {
		t.Errorf("[Case: %d] Expected %q, got %q", caseN, src.Message, message)
	}

	val = row[3]
	if len(src.Fields) == 0 {
		if val != nil {
			t.Fatalf("[Case: %d] Expected a nil field map, got %+v", caseN, val)
		}
	} else {
		var fields []orc.MapEntry
		fields, ok = val.([]orc.MapEntry)
		if !ok {
			t.Fatalf("[Case: %d] Field map stored in ORC cannot be cast to []orc.MapEntry, it's a %q", caseN, reflect.TypeOf(val))
		}
		if len(fields) != len(src.Fields) {
			t.Fatalf("[Case: %d] expected %d fields, got %d", caseN, len(src.Fields), len(fields))
		}
		for _, mapEntry := range fields {
			resultValue, ok := mapEntry.Value.(string)
			if !ok {
				t.Fatalf("[Case: %d] field value that cannot be cast to string, type: %q", caseN, reflect.TypeOf(mapEntry.Value))
			}
			resultKey, ok := mapEntry.Key.(string)
			if !ok {
				t.Fatalf("[Case: %d] field key that cannot be cast to string, type: %q", caseN, reflect.TypeOf(mapEntry.Key))
			}
			srcValue := src.Fields.Get(resultKey)
			if srcValue != resultValue {
				t.Errorf("[Case: %d] expected %q, got %q", caseN, srcValue, mapEntry.Value)
			}
		}
	}
}

type testCase struct {
	message string
	fields  log.Fielder
	err     error
}

func TestWriteRecord(t *testing.T) {
	testError := errors.New("My pants are on fire")
	cases := []testCase{
		{"hello", nil, nil}, // Basic entry
		{"morning", log.Fields{ // Entry with fields
			"underwear": "pants",
			"shoes":     "brogues",
			"fruit":     "banana",
		}, nil},
		{"afternoon", nil, testError},
	}
	for n, c := range cases {
		entry := makeTestEntry(c.message, c.fields, c.err)
		buff := testWriteRecordToORCBuffer(t, entry, n)
		testReadRowFromORCBuffer(t, buff, entry, n)
	}
}
