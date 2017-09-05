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

// CloserHandler is a specialisation of the
// github.com/apex/log.Handler interface to support a Close function.
type CloserHandler interface {
	log.Handler
	Close() error
}

// ArchiveFunc is a function type that is used to move an ORC log file
// from its current path to another location as part of the log
// rotation process.  The function will be called with the path of the
// current log file, it is the responsibility of an implementation to
// ensure that files are moved non-destructively, but the
// RotatingHandler guarantees that the file will be closed before an
// ArchvieFunc is called, and that no attemp to log will be made until
// after it has completed its work.
type ArchiveFunc func(oldPath string) error

// RotatingHandler is a github.com/apex/log.Handler implementation
// that uses a subordinate Handler to log to an ORC file, but
// additionally supports on demand rotation of this file via a Rotate
// function.  The RotatingHandler should only ever be constructed
// using the NewRotatingHandler function.
type RotatingHandler struct {
	mu       sync.Mutex
	path     string
	handler  CloserHandler
	archiveF ArchiveFunc
}

// NewRotatingHandler returns an instance of the RotatingHandler with
// a subordinate ORC Handler logging to the provided path.  Should
// Rotate be called then the provided ArchiveFunc will be used to move
// the current ORC log file out of the way before creating a new one
// at the same path and continuing to handle log entries.
func NewRotatingHandler(path string, archiveF ArchiveFunc) *RotatingHandler {
	handler := NewHandler(path)
	return &RotatingHandler{
		path:     path,
		handler:  handler,
		archiveF: archiveF,
	}
}

// HandleLog passes logging duty through to the subordinate ORC Handler.
func (h *RotatingHandler) HandleLog(e *log.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.handler.HandleLog(e)
}

// Rotate invokes the RotatingHandlers ArchiveFunc to move the current ORC log file out of the way and then creates a new Handler to deal with future logging.
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
// file.  Older archived files are pushed back to higher-number
// suffixes as the new archives are created.
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
