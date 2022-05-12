package server

import (
	"encoding/json"

	"github.com/gowerm123/jdb/pkg/database"
	"github.com/gowerm123/jdb/pkg/shared"
)

var (
	roundRobinPtr = 0

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
	switch inst.Operation {
	case shared.JdbSelect:
		buildAndConsume(inst, chId)
		break
	case shared.JdbCreate:
		partCols, _ := inst.Tags["partition-columns"]
		if err := database.CreateTable(inst.Targets[0], inst.Tags["schema"].(shared.Schema), partCols); err != nil {
			panic(err)
		}
		sendResponse(chId, "complete")
		break
	case shared.JdbDrop:
		database.DropTable(inst.Targets[0])
		sendResponse(chId, "complete")
		break
	case shared.JdbInsert:
		database.InsertValues(inst.Targets[0], inst.Tags["values"].([]shared.Blob))
		sendResponse(chId, "complete")
		break
	case shared.JdbList:
		tables := database.ListTables()
		bytes, _ := json.MarshalIndent(tables, "", "  ")
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
	}
	consumer := database.NewConsumer(inst.Targets[0], predicate)
	consumer.ConsumeAll()

	blobs := consumer.ReadAll()
	contents, err := json.MarshalIndent(blobs, "", "  ")

	if err != nil {
		panic(err)
	}

	shared.RespChannels[chId] <- string(contents)
}

func buildPredicate(table string, predicate shared.PredicatePayload) func(shared.Blob) shared.Blob {
	tables := database.ListTables()
	schema := tables[table].EntrySchema

	targetType := schema[predicate.Field]

	return func(blob shared.Blob) shared.Blob {
		if shared.Compare(blob[predicate.Field], predicate.Target, targetType, predicate.Comparator) {
			return blob
		}
		return nil
	}
}
