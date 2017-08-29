package apexorc

import (
	"os"
	"sync"

	"github.com/apex/log"
	"github.com/scritchley/orc"
)

type Handler struct {
	mu     sync.Mutex
	path   string
	writer *orc.Writer
}

func NewHandler(path string) *Handler {
	return &Handler{
		path: path,
	}
}

func (h *Handler) openORCFile() error {
	f, err := os.Open(h.path)
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

func (h *Handler) Close() error {
	return h.closeORCFile()
}
