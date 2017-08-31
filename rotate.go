package apexorc

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/apex/log"
)

type CloserHandler interface {
	log.Handler
	Close() error
}

type ArchiveFunc func(oldPath string) error

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

	err := h.handler.Close()
	if err != nil {
		return err
	}
	h.archiveF(h.path)
	h.handler = NewHandler(h.path)
	return nil
}

// NumericArchiveF is an ArchiveFunc that archives historic log files
// with numeric suffixes.  The lower the suffix the more recent the
// file.
func NumericArchiveF(oldPath string) error {
	var newPath string

	dir, fileName := path.Split(oldPath)
	extension := path.Ext(fileName)
	prefix := fileName[:len(fileName)-len(extension)]

	if extension == ".orc" {
		newPath = oldPath + ".1"
	} else {
		counter, err := strconv.Atoi(extension[1:])
		if err != nil {
			return err
		}
		newPath = fmt.Sprintf("%s.%d", filepath.Join(dir, prefix), counter+1)
	}

	// If the new path doesn't exist, we'll move the old file there and be done!
	_, err := os.Stat(newPath)
	if err == nil || !os.IsNotExist(err) {
		// This block should recursively move all existing logs back one number
		err = NumericArchiveF(newPath)
		if err != nil {
			return err
		}
	}

	return os.Rename(oldPath, newPath)

}
