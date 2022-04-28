package jdbql

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gowerm123/jdb/pkg/database"
)

const (
	jdbSelect      = "SELECT"
	jdbCreate      = "CREATE"
	jdbDrop        = "DROP"
	jdbInsert      = "INSERT"
	jdbInto        = "INTO"
	jdbValues      = "VALUES"
	jdbDelete      = "DELETE"
	jdbFrom        = "FROM"
	jdbTable       = "TABLE"
	jdbWhere       = "WHERE"
	jdbIdent       = "ident"
	jdbAs          = "AS"
	jdbPartitioned = "PARTITIONED"
	jdbOn          = "ON"
)

var (
	words       []string
	rawContents string
	truePtr     int
	iterPtr     int
	_cmd        Command
	tokenBuffer []string
	tagBuffer   []Tag
	prevToken   string
	currToken   string

	activeRequest *http.Request
	activeWriter  http.ResponseWriter

	keyWords    []string = []string{jdbSelect, jdbCreate, jdbDrop, jdbInsert, jdbDelete, jdbFrom, jdbTable, jdbAs, jdbInto, jdbValues}
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
		if len(tokenBuffer) > 0 {
			_cmd.addInstruction(parseFromTokenBuffer())
		}

		context := CreateContext(activeRequest, activeWriter, _cmd)
		err := context.Execute()
		if err != nil {
			panic(err)
		}
		return
	}

	switch currToken {
	case jdbSelect:
		addToTokenBuffer(jdbSelect)
		nextToken(false)
		optional("select-columns")
		expect(jdbFrom)
		break
	case jdbFrom:
		nextToken(false)
		ident()
		accept()
		break
	case jdbCreate:
		addToTokenBuffer(jdbCreate)
		nextToken(false)
		expect(jdbTable)
		break
	case jdbTable:
		nextToken(false)
		ident()
		accept()
		break
	case jdbAs:
		schema := schema()
		nextToken(false)
		tagBuffer = append(tagBuffer, Tag{key: "schema", value: schema})
		accept()
		break
	case jdbDrop:
		addToTokenBuffer(jdbDrop)
		nextToken(false)
		expect(jdbTable)
		break
	case jdbInsert:
		addToTokenBuffer(jdbInsert)
		nextToken(false)
		expect(jdbInto)
		break
	case jdbInto:
		nextToken(false)
		ident()
		expect(jdbValues)
		break
	case jdbValues:
		values := values()
		tagBuffer = append(tagBuffer, Tag{key: "values", value: values})
		nextToken(false)
		accept()
		break
	case jdbWhere:
		nextToken(false)
		predicate()
		break
	case jdbPartitioned:
		nextToken(false)
		expect(jdbOn)
		break
	case jdbOn:
		switch prevToken {
		case jdbPartitioned:
			nextToken(false)
			optional("partition-columns")
			accept()
			break
		}
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
	_cmd = Command{}
}

func optional(name string) {
	options := []string{currToken}
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
	nextToken(false)
}

func ident() {
	addToTokenBuffer(currToken)
	nextToken(true)
}

func isKeyword(cmd string) bool {
	return contains(keyWords, cmd)
}

func schema() database.Schema {
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

	var schema database.Schema
	if err := json.Unmarshal([]byte(subStr), &schema); err != nil {
		panic(err)
	}

	return schema
}

func values() []database.Blob {
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

	blobs := []database.Blob{}

	subStr := rawContents[truePtr:]
	split := strings.Split(subStr, "@@")
	for _, blobStr := range split {
		var blob database.Blob
		json.Unmarshal([]byte(blobStr), &blob)
		blobs = append(blobs, blob)
	}

	return blobs
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

	_predicate := database.BuildPredicate(field, comparator, target)
	addToTagBuffer("predicate", _predicate)

	nextToken(false)
	accept()
}

func addToTokenBuffer(str string) {
	tokenBuffer = append(tokenBuffer, str)
}

func parseFromTokenBuffer() Instruction {
	inst := Instruction{
		operation: tokenBuffer[1],
		target:    tokenBuffer[2],
	}

	for _, tag := range tagBuffer {
		inst.addTag(tag)
	}
	tagBuffer = []Tag{}
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
	tagBuffer = append(tagBuffer, Tag{
		key:   key,
		value: value,
	})
}
