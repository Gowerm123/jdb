package database

import (
	"errors"
	"strings"

	"github.com/gowerm123/jdb/pkg/shared"
)

var storageClient StorageClient

func InitClient(isTestEnvironment bool) {
	storageClient = ResolveClient(isTestEnvironment)
	storageClient.LoadTables()
}

func CreateTable(tableName string, schema shared.Schema, partitionColumns interface{}) error {
	partColumns := []string{}

	if partitionColumns != nil {
		partColumns = strings.Split(partitionColumns.(string), ",")
	}

	return storageClient.SaveTable(tableName, schema, partColumns)
}

func DropTable(tableName string) error {
	return storageClient.DropTable(tableName)
}

func InsertValues(target string, blobs []shared.Blob) error {
	schema := storageClient.GetTables()[target].EntrySchema
	if !schema.Validate(blobs...) {
		return errors.New("failed to validate schema")
	}
	return storageClient.InsertValues(target, blobs)
}

func SelectValues(query shared.Query) ([]shared.Blob, error) {
	return storageClient.SelectValues(query)
}

func ListTables() map[string]shared.TableEntry {
	return storageClient.GetTables()
}

func ResolveFile(table string) string {
	return storageClient.ResolveFile(table)
}

func GetClient() StorageClient {
	return storageClient
}
