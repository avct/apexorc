package apexorc

import (
	"encoding/json"
	"io"
	"os"
	"path"
	"sync"

	"github.com/apex/log"
)

// journalHandler pushes log.Entry instances out to a line-oriented
// JSON file (one valid JSON serialisation of a log.Entry per line).
type journalHandler struct {
	mu     sync.Mutex
	writer io.Writer
}

func newJournalHandler(w io.Writer) *journalHandler {
	return &journalHandler{writer: w}
}

func newJournalHandlerForPath(path string) (*journalHandler, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &journalHandler{writer: f}, nil
}

func (h *journalHandler) HandleLog(e *log.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = h.writer.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func (h *journalHandler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	wc, ok := h.writer.(io.WriteCloser)
	if !ok {
		// If it's not a WriterCloser this is a null op
		return nil
	}
	return wc.Close()
}

func makeJournalPathFromPath(srcPath string) string {
	ext := path.Ext(srcPath)
	extent := len(srcPath) - len(ext)
	return srcPath[:extent] + ".jrnl"
}
