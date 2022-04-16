package jdbql

import (
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

type Command []Instruction

func (cmd *Command) addInstructionFromStr(operation, target string) {
	*cmd = append(*cmd, Instruction{operation: operation, target: target})
}

func (cmd *Command) addInstruction(inst Instruction) {
	*cmd = append(*cmd, inst)
}

func (cmd *Command) Execute() error {
	for _, instruction := range *cmd {
		if err := cmd.execute(instruction); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *Command) execute(inst Instruction) error {
	var err error
	switch inst.operation {
	case jdbCreate:
		err = database.CreateTable(inst.target, inst.tags["schema"].(database.Schema))
		break
	case jdbDrop:
		err = database.DropTable(inst.target)
	case jdbInsert:
		err = database.InsertValues(inst.target, inst.tags["values"].([]database.Blob))
	}

	return err
}

func (inst *Instruction) addTag(tag Tag) {
	if inst.tags == nil {
		inst.tags = make(map[string]interface{})
	}
	inst.tags[tag.key] = tag.value
}
