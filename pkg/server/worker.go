package server

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gowerm123/jdb/pkg/database"
	"github.com/gowerm123/jdb/pkg/shared"
)

var (
	quit chan bool
)

func StartWorkers(n int) {
	shared.IdChannel = make(chan int)
	shared.CmdChannels = make([]chan shared.Instruction, n)
	shared.RespChannels = make([]chan string, n)
	for i := range shared.CmdChannels {
		shared.CmdChannels[i] = make(chan shared.Instruction)
		shared.RespChannels[i] = make(chan string)
	}
	shared.TableMappings = make(map[string]int)

	for i := 0; i < n; i++ {
		go listen(i)
	}
}

func listen(chId int) {
	for {
		select {
		case <-quit:
			return
		default:
			checkForTasks(chId)
		}
	}
}

func checkForTasks(chId int) {
	var inst = <-shared.CmdChannels[chId]
	shared.IdChannel <- chId
	fmt.Println(inst)
	switch inst.Operation {
	case shared.JdbSelect:
		buildAndConsume(inst, chId)
	case shared.JdbCreate:
		if val, ok := inst.Tags[shared.LOAD_TARGETS]; ok {
			if err := database.CreateTableFromInputFile(inst.Targets[0], val.([]string)); err != nil {
				panic(err)
			}
		} else {
			partCols := inst.Tags[shared.PARTITION_COLUMNS]
			schema, ok := inst.Tags["schema"].(shared.Schema)
			if !ok {
				schema = nil
			}
			if err := database.CreateTable(inst.Targets[0], schema, partCols); err != nil {
				panic(err)
			}
		}
		sendResponse(chId, "complete")
	case shared.JdbDrop:
		database.DropTable(inst.Targets[0])
		sendResponse(chId, "complete")

	case shared.JdbInsert:
		database.InsertValues(inst.Targets[0], inst.Tags["values"].([]shared.Blob))
		sendResponse(chId, "complete")
	case shared.JdbList:
		tables := database.ListTables()
		bytes, _ := json.MarshalIndent(tables, "", "  ")
		sendResponse(chId, string(bytes))
	case shared.JdbDescribe:
		tables := database.ListTables()
		entry := tables[inst.Targets[0]]
		bytes, _ := json.MarshalIndent(entry, "", "  ")
		sendResponse(chId, string(bytes))
	}
}

func sendResponse(chId int, response string) {
	shared.RespChannels[chId] <- response
}

func buildAndConsume(inst shared.Instruction, chId int) {
	var predicate func(shared.Blob) shared.Blob
	if preProPredicate, ok := inst.Tags["predicate"].(shared.PredicatePayload); !ok {
		predicate = nil
	} else {
		predicate = buildPredicate(inst.Targets[0], preProPredicate)
		if predicate == nil {
			shared.RespChannels[chId] <- string("WHERE clauses not allowed on schemaless tables")
			return
		}
	}

	var columnSelector func(shared.Blob) shared.Blob
	if columns, ok := inst.Tags[shared.SELECT_COLUMNS].([]string); !ok {
		columnSelector = nil
	} else {
		columnSelector = buildColumnSelector(inst.Targets[0], columns...)
		for _, column := range columns {
			if column == "*" {
				columnSelector = nil
			}
		}
	}
	consumer := database.NewConsumer(inst.Targets[0], predicate, columnSelector)
	consumer.ConsumeAll()

	blobs := consumer.ReadAll()
	contents, err := json.MarshalIndent(blobs, "", "  ")

	if err != nil {
		panic(err)
	}

	shared.RespChannels[chId] <- string(contents)
}

func buildColumnSelector(table string, columns ...string) func(shared.Blob) shared.Blob {
	return func(blob shared.Blob) shared.Blob {
		var newBlob shared.Blob = make(shared.Blob)

		for _, column := range columns {
			newBlob[column] = handleSubFields(column, blob)
		}

		return newBlob
	}
}

func handleSubFields(column string, blob map[string]interface{}) interface{} {
	if len(strings.Split(column, ".")) > 1 {
		rootObj := blob
		subColumns := strings.Split(column, ".")
		finalColumn := subColumns[len(subColumns)-1]
		subColumns = subColumns[:len(subColumns)-1]
		for _, sub := range subColumns {
			rootObj = rootObj[sub].(map[string]interface{})
		}

		return rootObj[finalColumn]
	}
	return blob[column]
}

func buildPredicate(table string, predicate shared.PredicatePayload) func(shared.Blob) shared.Blob {
	tables := database.ListTables()
	schema := tables[table].EntrySchema
	if schema == nil {
		return nil
	}

	return func(blob shared.Blob) shared.Blob {
		if shared.Compare(blob[predicate.Field], predicate.Target, predicate.Comparator) {
			return blob
		}
		return nil
	}
}
