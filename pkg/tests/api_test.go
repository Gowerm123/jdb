package tests

import (
	"encoding/json"
	"testing"

	"github.com/gowerm123/jdb/pkg/database"
)

func init() {
	database.InitClient(true)
}

func createTestTable() {
	tableName := "testTable"
	tableSchema := database.Schema{
		"col1": "string",
		"col2": "int",
	}

	database.CreateTable(tableName, tableSchema, nil)
}

func insertTestValue() {
	tableName := "testTable"
	value := `{"col1":"hello world!","col2":200}`
	var blob database.Blob

	json.Unmarshal([]byte(value), &blob)

	database.InsertValues(tableName, []database.Blob{blob})
}

func insertInvalidTestValue() error {
	tableName := "testTable"
	value := `{"bad":"hello world!","column":200}`
	var blob database.Blob

	json.Unmarshal([]byte(value), &blob)

	return database.InsertValues(tableName, []database.Blob{blob})
}

func selectAllFromTestTable() []database.Blob {
	blobs, _ := database.SelectValues(database.Query{
		Target:     "testTable",
		Columns:    []string{"*"},
		Predicates: nil,
	})

	return blobs
}

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
