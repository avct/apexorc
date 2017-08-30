package apexorc

import (
	"io"

	"github.com/apex/log"
	"github.com/scritchley/orc"
)

// entrySchema defines the columns of our ORC log file.
const entrySchema = "struct<timestamp:timestamp,level:string,message:string,fields:map<string,string>>"

// newWriter creates a new orc.Writer based on a provided io.Writer
// and with the entrySchema already set.
func newWriter(w io.Writer) (*orc.Writer, error) {
	schema, err := orc.ParseSchema(entrySchema)
	if err != nil {
		return nil, err
	}

	return orc.NewWriter(w, orc.SetSchema(schema))
}

// writeRecord will write a single row of data to a provided
// orc.Writer based on a provided log.Entry.
func writeRecord(w *orc.Writer, e *log.Entry) error {
	var strVal string
	var ok bool
	fields := make(map[string]string, len(e.Fields))
	for k, v := range e.Fields {
		strVal, ok = v.(string)
		if !ok {
			// Maybe we should log an error here?
			continue
		}
		fields[k] = strVal
	}
	return w.Write(e.Timestamp, e.Level.String(), e.Message, fields)
}
