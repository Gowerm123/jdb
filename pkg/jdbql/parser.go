package jdbql

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gowerm123/jdb/pkg/shared"
)

var (
	words       []string
	rawContents string
	truePtr     int
	iterPtr     int
	_cmd        shared.Command
	tokenBuffer []string
	tagBuffer   []shared.Tag
	prevToken   string
	currToken   string

	activeRequest *http.Request
	activeWriter  http.ResponseWriter

	keyWords []string = []string{
		shared.JdbSelect,
		shared.JdbCreate,
		shared.JdbDrop,
		shared.JdbInsert,
		shared.JdbDelete,
		shared.JdbFrom,
		shared.JdbTable,
		shared.JdbAs,
		shared.JdbInto,
		shared.JdbValues,
	}
	comparators []string = []string{">", "<", "=", "!=", "<=", ">="}
)

func AssignParserActives(req *http.Request, rw http.ResponseWriter) {
	activeRequest = req
	activeWriter = rw
}

func Parse(command string) {
	reset()
	words = strings.Split(command, " ")
	rawContents = command

	accept()
}

func accept() {
	if truePtr >= len(rawContents) {
		if len(tokenBuffer) > 0 || len(tagBuffer) > 0 {
			_cmd.AddInstruction(parseFromTokenBuffer())
		}

		context := shared.CreateContext(activeRequest, activeWriter, _cmd)
		err := context.Execute()
		if err != nil {
			panic(err)
		}
		return
	}

	switch currToken {
	case shared.JdbSelect:
		addToTokenBuffer(shared.JdbSelect)
		optional("select-columns")
		nextToken(false)
		expect(shared.JdbFrom)
		break
	case shared.JdbFrom:
		optional("targets")
		nextToken(false)
		accept()
		break
	case shared.JdbCreate:
		addToTokenBuffer(shared.JdbCreate)
		nextToken(false)
		expect(shared.JdbTable)
		break
	case shared.JdbTable:
		nextToken(false)
		ident()
		accept()
		break
	case shared.JdbAs:
		schema := schema()
		nextToken(false)
		tagBuffer = append(tagBuffer, shared.Tag{Key: "schema", Value: schema})
		accept()
		break
	case shared.JdbDrop:
		addToTokenBuffer(shared.JdbDrop)
		nextToken(false)
		expect(shared.JdbTable)
		break
	case shared.JdbInsert:
		addToTokenBuffer(shared.JdbInsert)
		nextToken(false)
		expect(shared.JdbInto)
		break
	case shared.JdbInto:
		nextToken(false)
		ident()
		expect(shared.JdbValues)
		break
	case shared.JdbValues:
		values := values()
		tagBuffer = append(tagBuffer, shared.Tag{Key: "values", Value: values})
		nextToken(false)
		accept()
		break
	case shared.JdbWhere:
		nextToken(false)
		predicate()
		break
	case shared.JdbPartitioned:
		nextToken(false)
		expect(shared.JdbOn)
		break
	case shared.JdbOn:
		switch prevToken {
		case shared.JdbPartitioned:
			optional("partition-columns")
			accept()
			break
		}
	case shared.JdbList:
		addToTokenBuffer(shared.JdbList)
		nextToken(false)
		ident()
		accept()
		break
	case shared.JdbGroup:
		nextToken(false)
		expect(shared.JdbBy)
		break
	case shared.JdbBy:
		optional("group-by-columns")
		accept()
		break
	default:
		addToTokenBuffer(currToken)
		nextToken(true)
		accept()
		break
	}
}

func expect(command string) {
	if currToken != command {
		fatal("unexpected token", currToken, "expected", command)
	}
	accept()
}

func nextToken(isIdent bool) {
	buff := ""
	if truePtr >= len(rawContents) {
		return
	}
	if rawContents[truePtr] == '\'' {
		truePtr++
		for truePtr < len(rawContents) && rawContents[truePtr] != '\'' {
			buff += string(rawContents[truePtr])
			truePtr++
		}
	} else {
		for truePtr < len(rawContents) && (rawContents[truePtr] != ' ') {
			buff += string(rawContents[truePtr])
			truePtr++
		}
		truePtr++
	}
	if !isIdent {
		prevToken = currToken
	}

	currToken = buff
}

func reset() {
	iterPtr = 0
	truePtr = 0
	_cmd = shared.Command{}
	tokenBuffer = []string{}
	tagBuffer = []shared.Tag{}
	currToken = ""
	prevToken = ""
}

func optional(name string) {
	options := []string{}
	tmpPtr := truePtr
	for true {
		token := ""
		for tmpPtr < len(rawContents) && rawContents[tmpPtr] != ',' && rawContents[tmpPtr] != ' ' {
			token += string(rawContents[tmpPtr])
			tmpPtr++
		}

		if isKeyword(token) {
			break
		}
		if token != "" {
			options = append(options, token)
		}
		if tmpPtr >= len(rawContents) || rawContents[tmpPtr] == ' ' {
			tmpPtr++
			break
		}

	}
	addToTagBuffer(name, options)
	truePtr = tmpPtr
	log.Println(options)
}

func ident() {
	var val []string = []string{}
	for _, tag := range tagBuffer {
		if tag.Key == "targets" {
			val = tag.Value.([]string)
		}
	}
	addToTagBuffer("targets", append(val, currToken))
	nextToken(true)
}

func isKeyword(cmd string) bool {
	return contains(keyWords, cmd)
}

func schema() shared.Schema {
	lbrPointer := 1
	tempPtr := truePtr + 1
	for tempPtr < len(rawContents) && lbrPointer > 0 {
		if rawContents[tempPtr] == '{' {
			lbrPointer++
		} else if rawContents[tempPtr] == '}' {
			lbrPointer--
		}
		tempPtr++
	}
	subStr := rawContents[truePtr:tempPtr]

	var schema shared.Schema
	if err := json.Unmarshal([]byte(subStr), &schema); err != nil {
		panic(err)
	}

	return schema
}

func values() []shared.Blob {
	tempPtr := truePtr + 1
	lPtr := tempPtr
	for rawContents[lPtr] != '{' {
		lPtr--
	}

	tmpPtr := truePtr + 1
	brackCtr := 1
	for tmpPtr < len(rawContents) {
		if brackCtr == 0 && rawContents[tmpPtr] == ' ' {
			truePtr = tmpPtr + 1
			break
		}
		if rawContents[tmpPtr] == '{' {
			brackCtr++
		} else if rawContents[tmpPtr] == '}' {
			brackCtr--
		}

		tmpPtr++
	}

	blobs := []shared.Blob{}

	subStr := rawContents[truePtr:]
	split := strings.Split(subStr, "@@")
	for _, blobStr := range split {
		var blob shared.Blob
		json.Unmarshal([]byte(blobStr), &blob)
		blobs = append(blobs, blob)
	}

	return blobs
}

func assignment(name string) {
	tmpPtr := truePtr
	buff := ""
	tempBuff := [][]string{}
	for true {
		firstTerm, secondTerm := "", ""

		for tmpPtr < len(rawContents) && (rawContents[tmpPtr] != ' ' || rawContents[tmpPtr] == '=') {
			buff += string(rawContents[tmpPtr])
			tmpPtr++
		}
		firstTerm = buff
		buff = ""

		if rawContents[tmpPtr] == ' ' {
			tmpPtr++
		}
		tmpPtr += 2
		for tmpPtr < len(rawContents) && (rawContents[tmpPtr] != ' ' || rawContents[tmpPtr] == ',') {
			buff += string(rawContents[tmpPtr])
			tmpPtr++
		}
		secondTerm = buff

		key, value := strings.ReplaceAll(firstTerm, " ", ""), strings.ReplaceAll(secondTerm, " ", "")

		tmp := []string{key, value}
		tempBuff = append(tempBuff, tmp)

		truePtr = tmpPtr

		if tmpPtr >= len(rawContents) || rawContents[tmpPtr] == ' ' {
			break
		} else {
			truePtr += 2
		}

	}

	addToTagBuffer(name, tempBuff)
}

func predicate() {
	field := currToken
	nextToken(false)
	comparator := currToken
	nextToken(false)
	target := currToken

	if target[0] == '\'' {
		target = target[1:]
		iterPtr++
		for currToken[len(currToken)-1] != '\'' {
			target += " " + currToken
		}

		target += " " + currToken[:len(currToken)-1]
	}

	_predicate := shared.BuildPredicate(field, comparator, target)
	addToTagBuffer("predicate", _predicate)

	nextToken(false)
	accept()
}

func addToTokenBuffer(str string) {
	tokenBuffer = append(tokenBuffer, str)
}

func parseFromTokenBuffer() shared.Instruction {
	inst := shared.Instruction{
		Operation: tokenBuffer[1],
	}

	for _, tag := range tagBuffer {
		inst.AddTag(tag)
	}
	tagBuffer = []shared.Tag{}
	tokenBuffer = []string{}
	return inst
}

func fatal(msgs ...string) {
	str := fmt.Sprint(msgs)

	panic(errors.New(str))
}

func contains(ls []string, tr string) bool {
	for _, lsTr := range ls {
		if tr == lsTr {
			return true
		}
	}
	return false
}

func addToTagBuffer(key string, value interface{}) {
	tagBuffer = append(tagBuffer, shared.Tag{
		Key:   key,
		Value: value,
	})
}
