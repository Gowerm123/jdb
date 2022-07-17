package database

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/gowerm123/jdb/pkg/shared"
)

type LocalStorageClient struct {
	tables map[string]shared.TableEntry
}

func (sc *LocalStorageClient) write(contents []byte, paths ...string) error {
	for _, path := range paths {
		err := os.WriteFile(path, contents, 0777)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *LocalStorageClient) append(contents []byte, paths ...string) error {
	for _, path := range paths {
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
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

func (sc *LocalStorageClient) SaveTable(name string, schema shared.Schema, partitionColumns []string) error {
	if _, ok := sc.tables[name]; ok {
		return fmt.Errorf("table %s already exists", name)
	}

	alpha := fmt.Sprintf("%s/alpha", shared.TruePath(name))
	beta := fmt.Sprintf("%s/beta", shared.TruePath(name))

	os.MkdirAll(shared.TruePath(name), 0777)

	if err := sc.write([]byte{}, alpha); err != nil {
		return err
	}

	if err := sc.write([]byte{}, beta); err != nil {
		return err
	}
	sc.appendToTableList(name, schema, partitionColumns)
	return nil
}

func (sc *LocalStorageClient) LoadTables() {
	contents, _ := ioutil.ReadFile(tableListPath())
	splitConts := strings.Split(string(contents), "\n")
	endMap := make(map[string]shared.TableEntry)

	for _, entry := range splitConts {
		if entry == "" {
			continue
		}
		var tableEntry shared.TableEntry
		json.Unmarshal([]byte(entry), &tableEntry)
		endMap[tableEntry.EntryName] = tableEntry
	}

	sc.tables = endMap
}

func (sc *LocalStorageClient) DropTable(tableName string) error {
	if _, ok := sc.tables[tableName]; !ok {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	if err := os.RemoveAll(shared.TruePath(tableName)); err != nil {
		return err
	}

	sc.removeFromTableList(tableName)
	return nil
}

func (sc *LocalStorageClient) InsertValues(target string, blobs []shared.Blob) (err error) {
	for _, blob := range blobs {
		bytes, err := json.Marshal(blob)
		if err != nil {
			return fmt.Errorf("error during json marshalling - %s", err.Error())
		}
		err = sc.append(bytes, sc.ResolveFile(target))
		if err != nil {
			return fmt.Errorf("error during write to table file - %s", err.Error())
		}
	}
	return err
}

func (sc *LocalStorageClient) SelectValues(query shared.Query) ([]shared.Blob, error) {
	var blobBuff []shared.Blob
	return blobBuff, nil
}

func (sc *LocalStorageClient) applyJoin(l []shared.Blob, r []shared.Blob, columns []string) []shared.Blob {
	buckets := make(map[string][]shared.Blob)
	for _, blob := range l {
		hash := sc.hash(blob, columns[0])

		tempBucket, ok := buckets[hash]
		if !ok {
			tempBucket = []shared.Blob{}
		}
		buckets[hash] = append(tempBucket, blob)
	}

	for _, blob := range r {
		hash := sc.hash(blob, columns[1])
		tempBucket, ok := buckets[hash]
		if !ok {
			tempBucket = []shared.Blob{}
		}
		buckets[hash] = append(tempBucket, blob)
	}

	var finalBlobs []shared.Blob
	for _, blob := range r {
		hash := sc.hash(blob, columns[0])
		tempBucket := buckets[hash]
		finalBlobs = append(finalBlobs, tempBucket...)
	}

	return finalBlobs
}

func (sc *LocalStorageClient) GetTables() map[string]shared.TableEntry {
	return sc.tables
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

func (sc *LocalStorageClient) appendToTableList(name string, schema shared.Schema, partitionColumns []string) {
	metadata := make(map[string]string)
	metadata["dir"] = shared.TruePath(name)
	entry := shared.TableEntry{
		EntryName:        name,
		EntrySchema:      schema,
		PartitionColumns: partitionColumns,
		Metadata:         metadata,
		CurrentMajor:     "alpha",
		CurrentMinor:     "beta",
	}
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
	ioutil.WriteFile(tableListPath(), []byte(contents), 0777)
}

func (sc *LocalStorageClient) hash(input shared.Blob, column string) string {
	var value interface{}
	getField(column, input, &value)
	str := fmt.Sprint(value)

	hasher := sha1.New()
	hasher.Write([]byte(str))

	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func (sc *LocalStorageClient) ResolveFile(table string) string {
	return fmt.Sprintf("%s/%s", shared.TruePath(table), sc.tables[table].CurrentMajor)
}

func tableListPath() string {
	return shared.TruePath(".tables")
}
