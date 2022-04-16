package database

import (
	"errors"
	"fmt"
	"log"
)

var storageClient StorageClient

const DIR = "/home/matt/jdb-dev"

var tables map[string]TableEntry

func init() {
	storageClient = ResolveClient()
	tables = storageClient.LoadTables()
	log.Println(tables)
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
