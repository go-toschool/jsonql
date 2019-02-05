package jsonql

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/src-d/go-mysql-server.v0/mem"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Database hold tables
type Database struct {
	tables map[string]sql.Table
}

// NewDatabase export a new Database
func NewDatabase(dbname, path string) (*mem.Database, error) {
	db := mem.NewDatabase(dbname)

	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read %s", path)
	}

	// tables := make(map[string]sql.Table)
	for _, fi := range fis {
		name := strings.ToLower(fi.Name())
		if fi.IsDir() || filepath.Ext(name) != ".json" {
			continue
		}
		filename := strings.ToLower(fi.Name())

		tableName := strings.TrimSuffix(name, ".json")
		table := mem.NewTable(tableName, sql.Schema{
			{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
			{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
			{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
			{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
		})
		db.AddTable(tableName, table)
		// open file
		f, err := os.Open(filepath.Join(path, filename))
		if err != nil {
			return nil, errors.Wrapf(err, "could not open %s", path)
		}
		defer f.Close()

		reader := bufio.NewReader(f)
		// iterate until EOF
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			}

			var p People

			data := strings.TrimSuffix(string(line), "\n")
			line = []byte(data)
			if err := json.Unmarshal(line, &p); err != nil {
				return nil, errors.Wrapf(err, "could not unmarshal %s", string(line))
			}

			ctx := sql.NewEmptyContext()
			// fill table with data
			row := sql.NewRow(p.Firstname, p.Lastname, []string{p.Phonenumber}, time.Now())
			table.Insert(ctx, row)
		}
	}

	return db, nil
}

// Name returns database name
func (d *Database) Name() string { return "jsonql" }

// Tables returns the information of all tables.
func (d *Database) Tables() map[string]sql.Table { return d.tables }
