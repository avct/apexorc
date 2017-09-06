package apexorc

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/apex/log"
)

type JournalHandler struct {
	mu     sync.Mutex
	writer io.Writer
}

func NewJournalHandler(w io.Writer) *JournalHandler {
	return &JournalHandler{writer: w}
}

func (h *JournalHandler) HandleLog(e *log.Entry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = h.writer.Write(b)
	if err != nil {
		return err
	}
	_, err = h.writer.Write([]byte{'\n'})
	if err != nil {
		return err
	}
	return nil
}
