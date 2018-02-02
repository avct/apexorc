package apexorc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/apex/log"
)

// CriticalRotationError is a special kind of error that can be
// returned by the Rotate() function of a RotatingHandler.  Should you
// encounter a CriticalRotationError you should assume that logging is
// no longer working and decide how to proceed.
//
// You can check an error to see if it is a CriticalRotationError by
// passing it to IsCriticalRotationError:
type CriticalRotationError struct {
	error
}

// // The error built-in interface type is the conventional interface for
// // representing an error condition, with the nil value representing no error.
// func (c CriticalRotationError) Error() string {
// 	return c.err.Error()
// }

// IsCriticalRotationError returns true if the error passed to it is a
// CriticalRotationErorr.
func IsCriticalRotationError(e error) bool {
	_, ok := e.(CriticalRotationError)
	return ok
}

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
	mu sync.Mutex // mu is the Mutex that is used in all
	// apex log handlers, it prevents
	// out-of-order logging.

	cmu sync.Mutex // cmu is a Mutex that protect the
	// process of converting a rotated log
	// journal into an ORC file.  By
	// separating this from the rotation
	// itself we can allow logging to
	// continue as soon as we've moved the
	// journal to a rotated position.
	journalPath string
	path        string
	handler     CloserHandler
	archiveF    ArchiveFunc
}

// NewRotatingHandler returns an instance of the RotatingHandler with
// a subordinate ORC Handler logging to the provided path.  Should
// Rotate be called then the provided ArchiveFunc will be used to move
// the current ORC log file out of the way before creating a new one
// at the same path and continuing to handle log entries.
func NewRotatingHandler(path string, archiveF ArchiveFunc) (*RotatingHandler, error) {
	journalPath := makeJournalPathFromPath(path)
	handler, err := newJournalHandlerForPath(journalPath)
	return &RotatingHandler{
		journalPath: journalPath,
		path:        path,
		handler:     handler,
		archiveF:    archiveF,
	}, err
}

// HandleLog passes logging duty through to the subordinate ORC Handler.
func (h *RotatingHandler) HandleLog(e *log.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.handler.HandleLog(e)
}

// Convert a journal file into an ORC file.  The intent is that this
// should only happen once all logging activity on the journal file is
// completed.
func (h *RotatingHandler) convertToORC(journalPath, orcPath string) error {
	h.cmu.Lock() // We lock out further conversion processes until
	// this one is finished.  The conusmer of this library is
	// expected to take care that calls to Rotate() usually happen
	// at intervals that exceed the time taken to completee
	// conversion so that a backlog of conversion processes
	// doesn't build-up.
	defer h.cmu.Unlock()

	logCtx := log.WithFields(
		log.Fields{
			"journalPath": journalPath,
			"function":    "convertToORC",
		})

	f, err := os.Open(journalPath)
	if err != nil {
		return err
	}

	orchandler := NewHandler(orcPath)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		// Note, per line error are logged, but otherwise
		// ignored - we want to convert every line we can.
		e := &log.Entry{}
		err := json.Unmarshal(scanner.Bytes(), e)
		if err != nil {
			logCtx.WithError(err).WithField("str", scanner.Text()).Error("Error unmarshalling during play back of journal")
		}
		err = orchandler.HandleLog(e)
		if err != nil {
			logCtx.WithError(err).Error("Error writing log entry to ORC")
		}
	}

	if err = scanner.Err(); err != nil {
		logCtx.WithError(err).Error("Error scanning journal")
	}

	err = orchandler.Close()
	if err != nil {
		logCtx.WithError(err).Error("Error closing the ORC file")
		return err
	}
	err = f.Close()
	if err != nil {
		logCtx.WithError(err).Error("Error closing the journal")
		return err
	}
	err = os.RemoveAll(path.Base(journalPath))
	if err != nil {
		logCtx.WithError(err).Error("Unable to remove temporary journal")
	}

	err = h.archiveF(orcPath)
	if err != nil {
		logCtx.WithError(err).Error("Error archiving ORC file")
		return err
	}

	return nil
}

// Rotate is a blocking call and will not return until an ORC file has
// been created.  Logging will only be blocked for the earliest part
// of the process, but subsequent calls to Rotate will not complete
// until earlier ones have already completed.
//
// The caller should check any returned error using
// IsCriticalRotationError.  If a CriticalRotationError is returned,
// logging will no longer work as the handler will not be unlocked.
// It is the callers responsiblity to decide on a course of action at
// that point (when all else fails, panic).
func (h *RotatingHandler) Rotate() error {
	h.mu.Lock()
	err := h.handler.Close()
	if err != nil {
		return CriticalRotationError{err}
	}
	dir, err := ioutil.TempDir("", "avocet-journal-")
	if err != nil {
		return CriticalRotationError{err}
	}
	workingPath := path.Join(dir, "working.jrnl")
	err = os.Rename(h.journalPath, workingPath)
	if err != nil {
		return CriticalRotationError{err}
	}

	h.handler, err = newJournalHandlerForPath(h.journalPath)
	if err != nil {
		return CriticalRotationError{err}
	}
	h.mu.Unlock()
	// At this point logging can continue
	return h.convertToORC(workingPath, h.path)
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
