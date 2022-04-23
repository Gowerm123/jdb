package database

import (
	"errors"
	"fmt"
)

type Predicate struct {
	field      string
	comparator string
	target     interface{}
}

func BuildPredicate(field, comparator string, target interface{}) Predicate {
	return Predicate{
		field:      field,
		comparator: comparator,
		target:     target,
	}
}

var storageClient StorageClient

const DIR = "/home/matt/jdb-dev"

var tables map[string]TableEntry

func init() {
	storageClient = ResolveClient()
	tables = storageClient.LoadTables()
}

func GetTables() map[string]TableEntry {
	return tables
}

func CreateTable(tableName string, schema Schema) error {
	if _, ok := tables[tableName]; ok {
		return errors.New(fmt.Sprintf("table %s already exists", tableName))
	}
	storageClient.SaveTable(tableName, schema)
	return nil
}

func DropTable(tableName string) error {
	if _, ok := tables[tableName]; !ok {
		return errors.New(fmt.Sprintf("table %s does not exist", tableName))
	}
	storageClient.DropTable(tableName)
	return nil
}

func InsertValues(target string, blobs []Blob) error {
	return storageClient.InsertValues(target, blobs)
}

func SelectValues(query Query) ([]Blob, error) {
	return storageClient.SelectValues(query)
}
