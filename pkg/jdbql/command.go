package jdbql

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gowerm123/jdb/pkg/database"
	"github.com/gowerm123/jdb/pkg/shared"
)

type Instruction struct {
	operation string
	targets   []string
	tags      map[string]interface{}
}

type Tag struct {
	key   string
	value interface{}
}

type CommandContext struct {
	req *http.Request
	rw  http.ResponseWriter
	cmd Command
}

func CreateContext(req *http.Request, rw http.ResponseWriter, cmd Command) CommandContext {
	cc := CommandContext{}

	cc.req = req
	cc.rw = rw
	cc.cmd = cmd

	return cc
}

func (cc *CommandContext) Execute() error {
	for _, instruction := range cc.cmd {
		if blobs, err := cc.cmd.execute(instruction); err != nil {
			return err
		} else {
			bytes, _ := json.Marshal(blobs)
			cc.rw.Write(bytes)
			cc.rw.WriteHeader(200)
		}
	}
	return nil
}

type Command []Instruction

func (cmd *Command) addInstructionFromStr(operation string, targets []string) {
	*cmd = append(*cmd, Instruction{operation: operation, targets: targets})
}

func (cmd *Command) addInstruction(inst Instruction) {
	*cmd = append(*cmd, inst)
}

func (cmd *Command) execute(inst Instruction) ([]shared.Blob, error) {
	log.Println(inst)
	var err error
	var blobs []shared.Blob
	switch inst.operation {
	case jdbCreate:
		err = database.CreateTable(inst.targets[0], inst.tags["schema"].(shared.Schema), inst.tags["partition-columns"])
		break
	case jdbDrop:
		err = database.DropTable(inst.targets[0])
	case jdbInsert:
		err = database.InsertValues(inst.targets[0], inst.tags["values"].([]shared.Blob))
	case jdbSelect:
		var predicates []database.Predicate
		_predicate, ok := (inst.tags["predicate"])
		if ok {
			predicates = append(predicates, _predicate.(database.Predicate))
		}

		blobs, err = database.SelectValues(database.Query{
			Targets:    inst.targets,
			Columns:    inst.tags["select-columns"].([]string),
			Predicates: predicates,
		})

	case jdbList:
		target := inst.targets[0]
		if target == "TABLES" {
			return toBlobList(database.ListTables()), nil
		}
	}

	return blobs, err
}

func toBlobList(tables map[string]database.TableEntry) (blobs []shared.Blob) {
	for key, _ := range tables {
		if key == "" {
			continue
		}
		blobs = append(blobs, shared.Blob{"tableName": key})
	}
	return blobs
}

func (inst *Instruction) addTag(tag Tag) {
	if inst.tags == nil {
		inst.tags = make(map[string]interface{})
	}
	if tag.key == "targets" {
		inst.targets = tag.value.([]string)
	}
	inst.tags[tag.key] = tag.value
}
