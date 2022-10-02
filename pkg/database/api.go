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

func CreateTableFromInputFile(tableName string, loadTargets []string) error {
	loadTarget := loadTargets[0]

	schema, err := shared.InterrogateSchema(loadTarget)
	if err != nil {
		panic(err)
	}

	return storageClient.SaveTableFromFile(tableName, loadTargets, schema)
}

func DropTable(tableName string) error {
	return storageClient.DropTable(tableName)
}

func InsertValues(target string, blobs []shared.Blob) error {

	schema := storageClient.GetTables()[target].EntrySchema

	if schema != nil && !schema.Validate(blobs...) {
		return errors.New("failed to validate schema")
	}
	return storageClient.InsertValues(target, blobs)
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
