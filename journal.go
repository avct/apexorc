package apexorc

import (
	"encoding/json"
	"io"
	"os"
	"path"
	"sync"

	"github.com/apex/log"
)

type JournalHandler struct {
	mu     sync.Mutex
	writer io.Writer
}

func NewJournalHandler(w io.Writer) *JournalHandler {
	return &JournalHandler{
		// mu:     &sync.Mutex{},
		writer: w}
}

func NewJournalHandlerForPath(path string) (*JournalHandler, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &JournalHandler{
		// mu:     &sync.Mutex{},
		writer: f}, nil
}

func (h *JournalHandler) HandleLog(e *log.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
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

func (h *JournalHandler) Close() error {
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
