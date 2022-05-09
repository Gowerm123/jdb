package database

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
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
		err := os.WriteFile(path, contents, os.ModeAppend)
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

func (sc *LocalStorageClient) SaveTable(name string, schema shared.Schema, partitionColumns []string) error {
	if _, ok := sc.tables[name]; ok {
		return errors.New(fmt.Sprintf("table %s already exists", name))
	}

	alpha := fmt.Sprintf("%s/alpha", shared.TruePath(name))
	beta := fmt.Sprintf("%s/beta", shared.TruePath(name))

	os.MkdirAll(shared.TruePath(name), 0644)

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
		var tableEntry shared.TableEntry
		json.Unmarshal([]byte(entry), &tableEntry)
		endMap[tableEntry.EntryName] = tableEntry
	}

	sc.tables = endMap
}

func (sc *LocalStorageClient) DropTable(tableName string) error {
	if _, ok := sc.tables[tableName]; !ok {
		return errors.New(fmt.Sprintf("table %s does not exist", tableName))
	}

	if err := os.Remove(shared.TruePath(tableName)); err != nil {
		return err
	}

	sc.removeFromTableList(tableName)
	return nil
}

func (sc *LocalStorageClient) InsertValues(target string, blobs []shared.Blob) error {
	tableEntry, ok := sc.tables[target]
	if !ok {
		return errors.New(fmt.Sprintf("table %s does not exist", target))
	}

	for _, blob := range blobs {
		if !tableEntry.EntrySchema.Validate(blob) {
			return errors.New(fmt.Sprintf("failed to validate schema for %v", blob))
		}
	}

	contents, err := ioutil.ReadFile(ResolveFile(target))
	split := strings.Split(string(contents), "\n")
	for _, blob := range blobs {
		blobStr, _ := json.Marshal(blob)
		split = append(split, string(blobStr))
	}
	err = ioutil.WriteFile(ResolveFile(target), []byte(strings.Join(split, "\n")), 0644)

	return err
}

func (sc *LocalStorageClient) SelectValues(query shared.Query) ([]shared.Blob, error) {
	var blobBuff []shared.Blob

	for _, target := range query.Targets {
		target = strings.ReplaceAll(target, ",", "")
		blobs := sc.collectBlobs(target, query.Columns, query.Predicates)

		blobBuff = append(blobBuff, blobs...)
	}
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
		tempBucket, _ := buckets[hash]
		finalBlobs = append(finalBlobs, tempBucket...)
	}

	return finalBlobs
}

func (sc *LocalStorageClient) GetTables() map[string]shared.TableEntry {
	return sc.tables
}

func (sc *LocalStorageClient) collectBlobs(target string, columns []string, predicates []shared.Predicate) []shared.Blob {
	filePath := sc.tables[target].Metadata["dir"]
	contents, _ := sc.read(filePath)

	blobs := []shared.Blob{}

	for _, line := range strings.Split(string(contents), "\n") {
		if line == "" {
			continue
		}
		var blob shared.Blob
		var endBlob shared.Blob = make(shared.Blob)
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

func (sc *LocalStorageClient) applyPredicates(target string, predicates []shared.Predicate, blobs []shared.Blob) []shared.Blob {
	schema := sc.tables[target].EntrySchema
	var keeps []bool = make([]bool, len(blobs))
	for _, predicate := range predicates {
		for ind, blob := range blobs {
			keeps[ind] = check(predicate, blob, schema)
		}
	}

	var newBlobs []shared.Blob

	for ind := range blobs {
		if keeps[ind] {
			newBlobs = append(newBlobs, blobs[ind])
		}
	}

	return newBlobs
}

func check(predicate shared.Predicate, blob shared.Blob, schema shared.Schema) bool {
	var target, targetType interface{}
	field, comparator, prTarget := predicate.GetFields()
	getField(field, blob, &target)
	getField(field, schema, &targetType)

	return compare(target, prTarget, targetType, comparator)
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
	entry := shared.NewTableEntry(name, schema, partitionColumns, metadata)
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

func (sc *LocalStorageClient) hash(input shared.Blob, column string) string {
	var value interface{}
	getField(column, input, &value)
	str := fmt.Sprint(value)

	hasher := sha1.New()
	hasher.Write([]byte(str))

	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func (sc *LocalStorageClient) ResolveFile(table string) string {
	return fmt.Sprintf("%s/alpha", shared.TruePath(table))
}

func tableListPath() string {
	return shared.TruePath(".tables")
}
