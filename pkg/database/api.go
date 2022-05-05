package database

import (
	"errors"
	"log"
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

	storageClient.SaveTable(tableName, schema, partColumns)
	return nil
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
	log.Println(query)
	return storageClient.SelectValues(query)
}

func ListTables() map[string]shared.TableEntry {
	return storageClient.GetTables()
}

func GetClient() StorageClient {
	return storageClient
}
