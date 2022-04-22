package database

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type LocalStorageClient struct{}

func (sc *LocalStorageClient) write(contents []byte, paths ...string) error {
	for _, path := range paths {
		err := os.WriteFile(path, contents, 644)
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

func (sc *LocalStorageClient) SaveTable(name string, schema Schema) error {
	if err := sc.write([]byte{}, truePath(name)); err != nil {
		return err
	}
	sc.appendToTableList(name, schema)
	return nil
}

func (sc *LocalStorageClient) LoadTables() map[string]TableEntry {
	contents, _ := ioutil.ReadFile(tableListPath())
	splitConts := strings.Split(string(contents), "\n")

	endMap := make(map[string]TableEntry)

	for _, entry := range splitConts {
		var tableEntry TableEntry
		json.Unmarshal([]byte(entry), &tableEntry)
		endMap[tableEntry.EntryName] = tableEntry
	}

	return endMap
}

func (sc *LocalStorageClient) DropTable(tableName string) error {
	if err := os.Remove(truePath(tableName)); err != nil {
		return err
	}

	sc.removeFromTableList(tableName)
	return nil
}

func (sc *LocalStorageClient) InsertValues(target string, blobs []Blob) error {
	contents, err := ioutil.ReadFile(truePath(target))
	split := strings.Split(string(contents), "\n")
	log.Println(string(contents))
	for _, blob := range blobs {
		blobStr, _ := json.Marshal(blob)
		split = append(split, string(blobStr))
	}

	err = ioutil.WriteFile(truePath(target), []byte(strings.Join(split, "\n")), 0644)

	return err
}

func (sc *LocalStorageClient) SelectValues(query Query) error {
	blobs := sc.collectBlobs(query)
	for _, blob := range blobs {
		log.Println(blob)
	}
	return nil
}

func (sc *LocalStorageClient) collectBlobs(query Query) []Blob {
	filePath := truePath(query.Target)
	contents, _ := sc.read(filePath)
	columns := strings.Split(query.Columns, ",")

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
			}
			endBlob[field] = blob[field]
		}

		blobs = append(blobs, endBlob)
	}
	return blobs
}

func (sc *LocalStorageClient) appendToTableList(name string, schema Schema) {
	entry := NewTableEntry(name, truePath(name), schema)
	tables[name] = entry
	sc.writeToTableListFile()
}

func (sc *LocalStorageClient) removeFromTableList(name string) {
	delete(tables, name)
	sc.writeToTableListFile()
}

func (sc *LocalStorageClient) writeToTableListFile() {
	contents := ""
	for _, table := range tables {
		if table.EntryName == "" {
			continue
		}
		str, _ := json.Marshal(table)
		contents += fmt.Sprintf("%s\n", str)
	}
	ioutil.WriteFile(tableListPath(), []byte(contents), 0644)
}

func truePath(path string) string {
	return fmt.Sprintf("%s/%s", DIR, path)
}

func tableListPath() string {
	return truePath(".tables")
}
