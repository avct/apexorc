// Package apexorc provides a handler for logging via
// github.com/apex/log to an ORC file.
package apexorc

import (
	"os"
	"sync"

	"github.com/apex/log"
	"github.com/scritchley/orc"
)

// Handler complies with the github.com/apex/log.Handler interface and
// can be passed to github.com/apex/log.SetHandler
type Handler struct {
	mu     sync.Mutex
	path   string
	writer *orc.Writer
}

// NewHandler returns a Handler which can log to an ORC file at the
// provided path.
func NewHandler(path string) *Handler {
	return &Handler{
		path: path,
	}
}

func (h *Handler) openORCFile() error {
	f, err := os.Create(h.path)
	if err != nil {
		return err
	}
	w, err := newWriter(f)
	if err != nil {
		return err
	}
	h.writer = w
	return nil
}

func (h *Handler) closeORCFile() error {
	err := h.writer.Close()
	if err != nil {
		return err
	}
	h.writer = nil
	return nil
}

// HandleLog recieves new log.Entrys and writes them to an ORC file or
// errors, as specified by the github.com/apex/log.Handler intefrace.
func (h *Handler) HandleLog(e *log.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.writer == nil {
		err := h.openORCFile()
		if err != nil {
			return err
		}
	}
	return writeRecord(h.writer, e)
}

// Close finalises the underlying ORC file.
func (h *Handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closeORCFile()
}
