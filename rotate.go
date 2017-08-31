package apexorc

import (
	"sync"

	"github.com/apex/log"
)

type CloserHandler interface {
	log.Handler
	Close() error
}

type ArchiveFunc func(oldPath string) string

type RotatingHandler struct {
	mu       sync.Mutex
	path     string
	handler  CloserHandler
	archiveF ArchiveFunc
}

func NewRotatingHandler(path string, archiveF ArchiveFunc) *RotatingHandler {
	handler := NewHandler(path)
	return &RotatingHandler{
		path:     path,
		handler:  handler,
		archiveF: archiveF,
	}
}

func (h *RotatingHandler) HandleLog(e *log.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.handler.HandleLog(e)
}

func (h *RotatingHandler) Rotate() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// err := h.handler.Close()
	// if err != nil {
	// 	return err
	// }
	// h.archiveF(h.path)
	// h.handler = NewHandler(h.path)
	return nil
}
