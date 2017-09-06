package apexorc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/apex/log"
)

func TestJournalHandleLog(t *testing.T) {
	buff := bytes.NewBuffer([]byte{})
	handler := newJournalHandler(buff)
	log.SetHandler(handler)
	logMsg := "Facial Hair Failure"
	testError := errors.New("Well bless my beard")
	log.WithError(testError).Error(logMsg)

	reader := bufio.NewReader(buff)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("Error reading line from journal buffer: %s", err)
	}

	e := &log.Entry{}
	err = json.Unmarshal(line, e)
	if err != nil {
		t.Fatalf("Error decoding record from journal: %s", err.Error())
	}
	if e.Level != log.ErrorLevel {
		t.Errorf("Expected journal entry to have Error log level, but got %s", e.Level.String())
	}
	if e.Message != logMsg {
		t.Errorf("Expected journal entry to have the message %q, but got %q", logMsg, e.Message)
	}
}

func TestMakeJournalPathFromPath(t *testing.T) {
	cases := []struct {
		Input    string
		Expected string
	}{
		{"/home/baron/log.orc", "/home/baron/log.jrnl"},
		{"/home/baron/log", "/home/baron/log.jrnl"},
		{"foo.xxes", "foo.jrnl"},
	}
	for cid, tcase := range cases {
		journalpath := makeJournalPathFromPath(tcase.Input)
		if journalpath != tcase.Expected {
			t.Errorf("[Case %d] Got %q, expected %q", cid, journalpath, tcase.Expected)
		}
	}
}
