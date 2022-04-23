package jdbql

import (
	"encoding/json"
	"net/http"

	"github.com/gowerm123/jdb/pkg/database"
)

type Instruction struct {
	operation string
	target    string
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

func (cmd *Command) addInstructionFromStr(operation, target string) {
	*cmd = append(*cmd, Instruction{operation: operation, target: target})
}

func (cmd *Command) addInstruction(inst Instruction) {
	*cmd = append(*cmd, inst)
}

func (cmd *Command) execute(inst Instruction) ([]database.Blob, error) {
	var err error
	var blobs []database.Blob
	switch inst.operation {
	case jdbCreate:
		err = database.CreateTable(inst.target, inst.tags["schema"].(database.Schema))
		break
	case jdbDrop:
		err = database.DropTable(inst.target)
	case jdbInsert:
		err = database.InsertValues(inst.target, inst.tags["values"].([]database.Blob))
	case jdbSelect:
		predicate, ok := inst.tags["predicate"]
		if !ok {
			blobs, err = database.SelectValues(database.Query{
				Target:  inst.target,
				Columns: inst.tags["options"].(string),
			})
		} else {
			blobs, err = database.SelectValues(database.Query{
				Target:     inst.target,
				Columns:    inst.tags["options"].(string),
				Predicates: []database.Predicate{predicate.(database.Predicate)},
			})
		}

	}

	return blobs, err
}

func (inst *Instruction) addTag(tag Tag) {
	if inst.tags == nil {
		inst.tags = make(map[string]interface{})
	}
	inst.tags[tag.key] = tag.value
}
