package tests

import (
	"encoding/json"
	"testing"

	"github.com/gowerm123/jdb/pkg/database"
	"github.com/gowerm123/jdb/pkg/shared"
)

func init() {
	database.InitClient(true)
}

func createTestTable() {
	tableName := "testTable"
	tableSchema := shared.Schema{
		"col1": "string",
		"col2": "int",
	}

	database.CreateTable(tableName, tableSchema, nil)
}

func insertTestValue() {
	tableName := "testTable"
	value := `{"col1":"hello world!","col2":200}`
	var blob shared.Blob

	json.Unmarshal([]byte(value), &blob)

	database.InsertValues(tableName, []shared.Blob{blob})
}

func insertInvalidTestValue() error {
	tableName := "testTable"
	value := `{"bad":"hello world!","column":200}`
	var blob shared.Blob

	json.Unmarshal([]byte(value), &blob)

	return database.InsertValues(tableName, []shared.Blob{blob})
}

func selectAllFromTestTable() []shared.Blob

func Test_CreateTableCallsStorageClientWithCorrectParams(t *testing.T) {
	createTestTable()

	tables := database.ListTables()

	if len(tables) == 0 {
		t.Fatal("CreateTable did not save table to tables map")
	}
}

func Test_InsertValuesAddsRecordToTable(t *testing.T) {
	createTestTable()
	insertTestValue()

	blobs := selectAllFromTestTable()

	if len(blobs) != 1 {
		t.Fatal("incorrect number of table entries")
	}
}

func Test_InsertInvalidSchemaThrowsError(t *testing.T) {
	createTestTable()
	err := insertInvalidTestValue()

	if err == nil {
		t.Fatal("insert invalid schema did not throw an error")
	}
}
