package sink

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// SQLTimestamp wraps a timestamp string so buildSQLRow emits TIMESTAMP '...' literal.
type SQLTimestamp struct {
	Val   string
	Valid bool
}

// SQLSink inserts values to a database in batch.
type SQLSink struct {
	maxBatchRows int

	insertHint string
	db         *sql.DB

	buf          bytes.Buffer
	bufferedRows int

	retryCount    int
	retryInterval time.Duration
}

var _ Sink = &SQLSink{}

// NewSQLSink creates a sink that inserts values to a database in batch.
func NewSQLSink(db *sql.DB, hint string, retryCount int, retryInterval time.Duration) *SQLSink {
	return &SQLSink{
		maxBatchRows:  1024,
		insertHint:    hint,
		db:            db,
		retryCount:    retryCount,
		retryInterval: retryInterval,
	}
}

func buildSQLRow(values []interface{}) string {
	var buf bytes.Buffer
	buf.WriteString("(")
	for i, v := range values {
		if i > 0 {
			buf.WriteString(",")
		}
		ty := reflect.TypeOf(v)
		if ty == nil {
			buf.WriteString("NULL")
			continue
		}
		switch ty.Kind() {
		case reflect.String:
			// TODO: Escape string correctly
			_, _ = fmt.Fprintf(&buf, "'%s'", v)
			continue
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			_, _ = fmt.Fprintf(&buf, "%d", v)
			continue
		case reflect.Float32, reflect.Float64:
			_, _ = fmt.Fprintf(&buf, "%f", v)
			continue
		}
		switch v := v.(type) {
		case sql.NullString:
			if v.Valid {
				_, _ = fmt.Fprintf(&buf, "'%s'", v.String)
			} else {
				buf.WriteString("NULL")
			}
		case sql.NullInt64:
			if v.Valid {
				_, _ = fmt.Fprintf(&buf, "%d", v.Int64)
			} else {
				buf.WriteString("NULL")
			}
		case sql.NullFloat64:
			if v.Valid {
				_, _ = fmt.Fprintf(&buf, "%f", v.Float64)
			} else {
				buf.WriteString("NULL")
			}
		case SQLTimestamp:
			if v.Valid {
				_, _ = fmt.Fprintf(&buf, "TIMESTAMP '%s'", v.Val)
			} else {
				buf.WriteString("NULL")
			}
		default:
			panic(fmt.Sprintf("unsupported type: %T", v))
		}
	}
	buf.WriteString(")")
	return buf.String()
}

// WriteRow writes a row to the database. The writing attempt may be deferred until reaching a batch.
func (s *SQLSink) WriteRow(ctx context.Context, values ...interface{}) error {
	row := buildSQLRow(values)

	if s.bufferedRows == 0 {
		s.buf.WriteString(s.insertHint)
		s.buf.WriteString(" ")
		s.buf.WriteString(row)
	} else {
		s.buf.WriteString(", ")
		s.buf.WriteString(row)
	}

	s.bufferedRows++
	if s.bufferedRows >= s.maxBatchRows {
		return s.Flush(ctx)
	}

	return nil
}

// Flush writes any buffered data to the db.
func (s *SQLSink) Flush(ctx context.Context) error {
	if s.buf.Len() == 0 {
		return nil
	}

	const gridgainMaxRetries = 10
	var err error
	for i := 0; i < 1+s.retryCount+gridgainMaxRetries; i++ {
		_, err = s.db.ExecContext(ctx, s.buf.String())
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "Error 1062: Duplicate entry") {
			if i == 0 {
				return fmt.Errorf("exec statement error: %v", err)
			}
			break
		}
		// GridGain: optimistic lock conflict on concurrent INSERTs to the same table.
		// The "try again later" hint means this is a transient conflict, not a real PK duplicate.
		if strings.Contains(err.Error(), "PK unique constraint is violated") &&
			strings.Contains(err.Error(), "try again later") {
			fmt.Printf("exec statement error: %v, retrying (%d/%d)...\n", err, i+1, gridgainMaxRetries)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// Generic retry (controlled by retryCount config)
		if i < s.retryCount {
			fmt.Printf("exec statement error: %v, try again later...\n", err)
			time.Sleep(s.retryInterval)
			continue
		}
		break
	}

	s.bufferedRows = 0
	s.buf.Reset()

	return nil
}

func (s *SQLSink) Close(ctx context.Context) error {
	return s.Flush(ctx)
}
