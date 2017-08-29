package apexorc

import (
	"io"

	"github.com/apex/log"
	"github.com/scritchley/orc"
)

const entrySchema = "struct<timestamp:timestamp,level:string,message:string,fields:map<string,string>>"

func newWriter(w io.Writer) (*orc.Writer, error) {
	schema, err := orc.ParseSchema(entrySchema)
	if err != nil {
		return nil, err
	}

	return orc.NewWriter(w, orc.SetSchema(schema))
}

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
