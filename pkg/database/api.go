package database

import (
	"errors"
	"fmt"
	"strings"
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

func CreateTable(tableName string, schema Schema, partitionColumns interface{}) error {
	partColumns := []string{}

	if partitionColumns != nil {
		partColumns = strings.Split(partitionColumns.(string), ",")
	}

	if _, ok := tables[tableName]; ok {
		return errors.New(fmt.Sprintf("table %s already exists", tableName))
	}
	storageClient.SaveTable(tableName, schema, partColumns)
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
	tableEntry, ok := tables[target]
	if !ok {
		return errors.New(fmt.Sprintf("table %s does not exist", target))
	}

	for _, blob := range blobs {
		if !tableEntry.EntrySchema.Validate(blob) {
			return errors.New(fmt.Sprintf("failed to validate schema for %v", blob))
		}
	}

	return storageClient.InsertValues(target, blobs)
}

func SelectValues(query Query) ([]Blob, error) {
	return storageClient.SelectValues(query)
}
