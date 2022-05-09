package server

import (
	"log"

	"github.com/gowerm123/jdb/pkg/database"
	"github.com/gowerm123/jdb/pkg/shared"
)

var (
	roundRobinPtr = 0

	quit chan bool
)

func StartWorkers(n int) {
	shared.Channels = make([]chan shared.Instruction, n)
	for i := range shared.Channels {
		shared.Channels[i] = make(chan shared.Instruction)
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
	log.Println("listening on channelId", chId)
	var inst = <-shared.Channels[chId]
	switch inst.Operation {
	case shared.JdbSelect:
		consumer := database.NewConsumer(inst.Targets[0])
		consumer.ConsumeAll()
		break
	case shared.JdbCreate:
		partCols, _ := inst.Tags["partition-columns"]
		if err := database.CreateTable(inst.Targets[0], inst.Tags["schema"].(shared.Schema), partCols); err != nil {
			panic(err)
		}
		break
	case shared.JdbDrop:
		database.DropTable(inst.Targets[0])
		break
	case shared.JdbInsert:
		database.InsertValues(inst.Targets[0], inst.Tags["values"].([]shared.Blob))
		break
	}

}
