package shared

import (
	"encoding/json"
	"log"
	"net/http"
)

type Instruction struct {
	Operation string
	Targets   []string
	Tags      map[string]interface{}
}

type Tag struct {
	Key   string
	Value interface{}
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
	*cmd = append(*cmd, Instruction{Operation: operation, Targets: targets})
}

func (cmd *Command) AddInstruction(inst Instruction) {
	*cmd = append(*cmd, inst)
}

func (cmd *Command) execute(inst Instruction) ([]Blob, error) {
	log.Println(inst)
	var err error
	var blobs []Blob
	switch inst.Operation {
	case JdbCreate:
		break
	case JdbDrop:
	case JdbInsert:
	}
	return blobs, err
}

func (inst *Instruction) AddTag(tag Tag) {
	if inst.Tags == nil {
		inst.Tags = make(map[string]interface{})
	}
	if tag.Key == "targets" {
		inst.Targets = tag.Value.([]string)
	}
	inst.Tags[tag.Key] = tag.Value
}

func toBlobList(tables map[string]TableEntry) (blobs []Blob) {
	for key, _ := range tables {
		if key == "" {
			continue
		}
		blobs = append(blobs, Blob{"tableName": key})
	}
	return blobs
}
