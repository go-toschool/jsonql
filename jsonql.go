package jsonql

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fatih/structs"
	"github.com/pkg/errors"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Database hold tables
type Database struct {
	tables map[string]sql.Table
}

// NewDatabase export a new Database
func NewDatabase(path string) (*Database, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read %s", path)
	}

	tables := make(map[string]sql.Table)
	for _, fi := range fis {
		name := strings.ToLower(fi.Name())
		if fi.IsDir() || filepath.Ext(name) != ".json" {
			continue
		}
		t, err := newTable(filepath.Join(path, name))
		if err != nil {
			return nil, errors.Wrapf(err, "could not create table from %s", name)
		}
		tables[strings.TrimSuffix(name, ".json")] = t
	}

	return &Database{tables}, nil
}

// Name returns database name
func (d *Database) Name() string { return "jsonql" }

// Tables returns the information of all tables.
func (d *Database) Tables() map[string]sql.Table { return d.tables }

type table struct {
	name   string
	schema sql.Schema
	path   string

	partitions map[string][]sql.Row
	keys       [][]byte

	insert int

	filters    []sql.Expression
	projection []string
	columns    []int
	lookup     sql.IndexLookup
}

var _ sql.Table = (*table)(nil)

func newTable(path string) (sql.Table, error) {
	names := (&People{}).StructAttrName()

	var schema []*sql.Column
	for _, header := range names {
		schema = append(schema, &sql.Column{
			Name:   header,
			Type:   sql.Text,
			Source: path,
		})
	}

	var keys [][]byte
	var partitions = map[string][]sql.Row{}

	numPartitions := 1

	for i := 0; i < numPartitions; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, []byte(key))
		partitions[key] = []sql.Row{}
	}

	name := strings.TrimSuffix(filepath.Base(path), ".json")

	return &table{
		name:       name,
		schema:     schema,
		path:       path,
		partitions: partitions,
		keys:       keys,
	}, nil
}

func (t *table) Name() string       { return t.name }
func (t *table) String() string     { return t.name }
func (t *table) Schema() sql.Schema { return t.schema }

func (t *table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	var keys [][]byte

	return &partitionIter{keys: keys}, nil
}

// PartitionCount implements the sql.PartitionCounter interface.
func (t *table) PartitionCount(ctx *sql.Context) (int64, error) {
	fmt.Println("PartitionCount", int64(len(t.partitions)))
	return int64(len(t.partitions)), nil
}

func (t *table) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	fmt.Println("PartitionRows")
	f, err := os.Open(t.path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open %s", t.path)
	}

	r := bufio.NewReader(f)
	ri := &rowIter{
		f,
		r,
	}

	return ri, nil
}

type rowIter struct {
	io.Closer
	*bufio.Reader
}

var _ sql.RowIter = (*rowIter)(nil)

// this method reads from file and fill rows.
func (r *rowIter) Next() (sql.Row, error) {
	fmt.Println("rowIter -> Next")
	line, _, err := r.ReadLine()
	if err == io.EOF {
		return nil, err
	} else if err != nil {
		return nil, errors.Wrap(err, "could not read row")
	}

	var p People

	line = []byte(strings.TrimSuffix(string(line), "\n"))
	if err := json.Unmarshal(line, &p); err != nil {
		return nil, errors.Wrapf(err, "could not unmarshal %s", string(line))
	}

	cols := p.GetFieldsLen()
	row := make(sql.Row, cols)
	m := structs.Map(p)
	for i := 0; i < cols; i++ {
		row[i] = m[p.GetFields(i)]
	}

	return row, nil
}

// func (r *rowIter) Close() error {
// 	if r.indexValues == nil {
// 		return nil
// 	}

// 	return r.indexValues.Close()
// }

// func (r *rowIter) getRow() (sql.Row, error) {
// 	if r.indexValues != nil {
// 		return r.getFromIndex()
// 	}

// 	if r.pos >= len(r.rows) {
// 		return nil, io.EOF
// 	}

// 	row := r.rows[r.pos]
// 	r.pos++
// 	return row, nil
// }

// func (r *rowIter) getFromIndex() (sql.Row, error) {
// 	data, err := r.indexValues.Next()
// 	if err != nil {
// 		return nil, err
// 	}

// 	value, err := decodeIndexValue(data)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return r.rows[value.Pos], nil
// }

type partitionIter struct {
	done bool
	keys [][]byte
	pos  int
}

func (p *partitionIter) Close() error { return nil }

type partition struct {
	key []byte
}

func (p *partition) Key() []byte { return []byte{'@'} }

func (p *partitionIter) Next() (sql.Partition, error) {
	fmt.Println("partitionIter -> Next")
	if p.done {
		return nil, io.EOF
	}

	// key := p.keys[p.pos]
	p.done = true
	p.pos++
	return &partition{}, nil
}

type indexValue struct {
	Key string
	Pos int
}

func decodeIndexValue(data []byte) (*indexValue, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var value indexValue
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}

	return &value, nil
}

func encodeIndexValue(value *indexValue) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
