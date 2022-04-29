package database

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/gowerm123/jdb/pkg/configs"
)

type LocalStorageClient struct {
	tables map[string]TableEntry
}

func (sc *LocalStorageClient) write(contents []byte, paths ...string) error {
	for _, path := range paths {
		err := os.WriteFile(path, contents, 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *LocalStorageClient) read(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func (sc *LocalStorageClient) append(contents []byte, paths ...string) error {
	for _, path := range paths {
		file, err := os.OpenFile(path, os.O_APPEND, 0644)
		if err != nil {
			return err
		}

		_, err = file.Write(contents)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *LocalStorageClient) SaveTable(name string, schema Schema, partitionColumns []string) error {
	if _, ok := sc.tables[name]; ok {
		return errors.New(fmt.Sprintf("table %s already exists", name))
	}

	if err := sc.write([]byte{}, truePath(name)); err != nil {
		return err
	}

	sc.appendToTableList(name, schema, partitionColumns)
	return nil
}

func (sc *LocalStorageClient) LoadTables() {
	contents, _ := ioutil.ReadFile(tableListPath())
	splitConts := strings.Split(string(contents), "\n")
	endMap := make(map[string]TableEntry)

	for _, entry := range splitConts {
		var tableEntry TableEntry
		json.Unmarshal([]byte(entry), &tableEntry)
		endMap[tableEntry.EntryName] = tableEntry
	}

	sc.tables = endMap
}

func (sc *LocalStorageClient) DropTable(tableName string) error {
	if _, ok := sc.tables[tableName]; !ok {
		return errors.New(fmt.Sprintf("table %s does not exist", tableName))
	}

	if err := os.Remove(truePath(tableName)); err != nil {
		return err
	}

	sc.removeFromTableList(tableName)
	return nil
}

func (sc *LocalStorageClient) InsertValues(target string, blobs []Blob) error {
	tableEntry, ok := sc.tables[target]
	if !ok {
		return errors.New(fmt.Sprintf("table %s does not exist", target))
	}

	for _, blob := range blobs {
		if !tableEntry.EntrySchema.Validate(blob) {
			return errors.New(fmt.Sprintf("failed to validate schema for %v", blob))
		}
	}

	contents, err := ioutil.ReadFile(truePath(target))
	split := strings.Split(string(contents), "\n")
	for _, blob := range blobs {
		blobStr, _ := json.Marshal(blob)
		split = append(split, string(blobStr))
	}

	err = ioutil.WriteFile(truePath(target), []byte(strings.Join(split, "\n")), 0644)

	return err
}

func (sc *LocalStorageClient) SelectValues(query Query) ([]Blob, error) {
	var blobBuff []Blob

	for _, target := range query.Targets {
		blobs := sc.collectBlobs(target, query.Columns, query.Predicates)

		blobBuff = append(blobBuff, blobs...)
	}
	return blobBuff, nil
}

func (sc *LocalStorageClient) GetTables() map[string]TableEntry {
	return sc.tables
}

func (sc *LocalStorageClient) collectBlobs(target string, columns []string, predicates []Predicate) []Blob {
	filePath := sc.tables[target].Metadata["dir"]
	contents, _ := sc.read(filePath)

	blobs := []Blob{}

	for _, line := range strings.Split(string(contents), "\n") {
		if line == "" {
			continue
		}
		var blob Blob
		var endBlob Blob = make(Blob)
		json.Unmarshal([]byte(line), &blob)
		for _, field := range columns {
			if field == "*" {
				for field2 := range blob {
					endBlob[field2] = blob[field2]
				}
				break
			} else {
				var value interface{}
				getField(field, blob, &value)
				endBlob[field] = value
			}
		}

		blobs = append(blobs, endBlob)
	}

	if len(predicates) > 0 {
		blobs = sc.applyPredicates(target, predicates, blobs)
	}
	return blobs
}

func (sc *LocalStorageClient) applyPredicates(target string, predicates []Predicate, blobs []Blob) []Blob {
	schema := sc.tables[target].EntrySchema
	var keeps []bool = make([]bool, len(blobs))
	for _, predicate := range predicates {
		for ind, blob := range blobs {
			keeps[ind] = check(predicate, blob, schema, predicate.comparator)
		}
	}

	var newBlobs []Blob

	for ind := range blobs {
		if keeps[ind] {
			newBlobs = append(newBlobs, blobs[ind])
		}
	}

	return newBlobs
}

func check(predicate Predicate, blob Blob, schema Schema, comparator string) bool {
	var target, targetType interface{}
	getField(predicate.field, blob, &target)
	getField(predicate.field, schema, &targetType)

	return compare(target, predicate.target, targetType, comparator)
}

func getField(field string, blob map[string]interface{}, target *interface{}) {
	spl := strings.Split(field, ".")

	var currMap map[string]interface{} = blob
	for ind, field := range spl {
		if ind != len(spl)-1 {
			currMap = currMap[field].(map[string]interface{})
		} else {
			*target = currMap[field]
		}
	}
}

func (sc *LocalStorageClient) appendToTableList(name string, schema Schema, partitionColumns []string) {
	metadata := make(map[string]string)
	metadata["dir"] = truePath(name)
	entry := NewTableEntry(name, schema, partitionColumns, metadata)
	sc.tables[name] = entry
	sc.writeToTableListFile()
}

func (sc *LocalStorageClient) removeFromTableList(name string) {
	delete(sc.tables, name)
	sc.writeToTableListFile()
}

func (sc *LocalStorageClient) writeToTableListFile() {
	contents := ""
	for _, table := range sc.tables {
		if table.EntryName == "" {
			continue
		}
		str, _ := json.Marshal(table)
		contents += fmt.Sprintf("%s\n", str)
	}
	ioutil.WriteFile(tableListPath(), []byte(contents), 0644)
}

func truePath(path string) string {
	basePath := configs.GetConfig(configs.BaseDirectoryPath)
	return fmt.Sprintf("%s/%s", basePath, path)
}

func tableListPath() string {
	return truePath(".tables")
}
