package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var order = flag.String("order", "time,msg", "Order of fields (missing will be sorted alphanumerically after this list")

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		log.Fatalf("usage: %s <file|->", os.Args[0])
	}

	var inStream io.Reader
	if args[0] == "-" {
		inStream = os.Stdin
	} else {
		f, err := os.Open(args[0])
		if err != nil {
			log.Fatalf("open %s err: %s", args[0], err)
		}
		inStream = f
		defer f.Close()
	}

	orderedFields := strings.Split(*order, ",")
	orderedFieldIndex := make(map[string]int)
	for i, f := range orderedFields {
		orderedFieldIndex[f] = i
	}

	dec := json.NewDecoder(inStream)
	dec.UseNumber()
	for {
		var rec map[string]interface{}
		err := dec.Decode(&rec)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		sortedFields := make([]string, 0, len(rec))
		for k := range rec {
			sortedFields = append(sortedFields, k)
		}

		sort.Slice(sortedFields, func(i, j int) bool {
			idxA, inOrderA := orderedFieldIndex[sortedFields[i]]
			idxB, inOrderB := orderedFieldIndex[sortedFields[j]]

			if inOrderA && inOrderB {
				return idxA < idxB
			} else if inOrderA {
				return true
			} else if inOrderB {
				return false
			}

			return sortedFields[i] < sortedFields[j]
		})

		var b strings.Builder
		for i, field := range sortedFields {
			val := rec[field]
			fmt.Fprintf(&b, "%s=%s", field, formatLogfmtValue(val))
			if i < len(sortedFields) {
				b.WriteByte(' ')
			}
		}

		fmt.Println(b.String())
	}
}

// formatValue formats a value for serialization
func formatLogfmtValue(value interface{}) string {
	if value == nil {
		return "nil"
	}

	if t, ok := value.(time.Time); ok {
		// Performance optimization: No need for escaping since the provided
		// timeFormat doesn't have any escape characters, and escaping is
		// expensive.
		return t.Format(timeFormat)
	}
	value = formatShared(value)
	switch v := value.(type) {
	case bool:
		return strconv.FormatBool(v)
	case float32:
		return strconv.FormatFloat(float64(v), floatFormat, 3, 64)
	case float64:
		return strconv.FormatFloat(v, floatFormat, 3, 64)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value)
	case string:
		return escapeString(v)
	default:
		return escapeString(fmt.Sprintf("%+v", value))
	}
}

var stringBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func escapeString(s string) string {
	needsQuotes := false
	needsEscape := false
	for _, r := range s {
		if r <= ' ' || r == '=' || r == '"' {
			needsQuotes = true
		}
		if r == '\\' || r == '"' || r == '\n' || r == '\r' || r == '\t' {
			needsEscape = true
		}
	}
	if needsEscape == false && needsQuotes == false {
		return s
	}
	e := stringBufPool.Get().(*bytes.Buffer)
	e.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\', '"':
			e.WriteByte('\\')
			e.WriteByte(byte(r))
		case '\n':
			e.WriteString("\\n")
		case '\r':
			e.WriteString("\\r")
		case '\t':
			e.WriteString("\\t")
		default:
			e.WriteRune(r)
		}
	}
	e.WriteByte('"')
	var ret string
	if needsQuotes {
		ret = e.String()
	} else {
		ret = string(e.Bytes()[1 : e.Len()-1])
	}
	e.Reset()
	stringBufPool.Put(e)
	return ret
}

const (
	timeFormat  = "2006-01-02T15:04:05-0700"
	floatFormat = 'f'
)

func formatShared(value interface{}) (result interface{}) {
	defer func() {
		if err := recover(); err != nil {
			if v := reflect.ValueOf(value); v.Kind() == reflect.Ptr && v.IsNil() {
				result = "nil"
			} else {
				panic(err)
			}
		}
	}()

	switch v := value.(type) {
	case time.Time:
		return v.Format(timeFormat)

	case error:
		return v.Error()

	case fmt.Stringer:
		return v.String()

	default:
		return v
	}
}
