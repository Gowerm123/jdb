package jdbql

const (
	CHAR_LPAREN    = '('
	CHAR_RPAREN    = ')'
	CHAR_SEMICOLON = ';'
	CHAR_PLUS      = '+'
	CHAR_MINUS     = '-'
	CHAR_NOT       = '!'
	CHAR_COMMA     = ','
	CHAR_QUOTE     = '"'
	CHAR_SQUOTE    = '\''
	CHAR_LTHAN     = '<'
	CHAR_GTHAN     = '>'
	CHAR_EQUALS    = '='
	CHAR_WSP       = ' '
)

var reservedSyms = []byte{CHAR_SEMICOLON, CHAR_SQUOTE, CHAR_QUOTE, CHAR_PLUS, CHAR_MINUS, CHAR_NOT, CHAR_COMMA, CHAR_WSP, CHAR_EQUALS, CHAR_LPAREN, CHAR_RPAREN, CHAR_LTHAN, CHAR_GTHAN}

func Lex(content []byte) []JdbToken {
	return lex(content)
}

func lex(content []byte) []JdbToken {
	ptr := 0
	tokenList := []JdbToken{}
	for ptr < len(content) {
		sym := collectSym(&ptr, content)
		switch string(sym) {
		case string(CHAR_SEMICOLON):
			addToTokenList(sym, TOKEN_TYPE_SEMICOLON, &tokenList)
		case string(CHAR_QUOTE):
			addToTokenList(sym, TOKEN_TYPE_QUOTE, &tokenList)
		case string(CHAR_SQUOTE):
			addToTokenList(sym, TOKEN_TYPE_TIC, &tokenList)
		case string(CHAR_PLUS), string(CHAR_MINUS):
			addToTokenList(sym, TOKEN_TYPE_BINOP, &tokenList)
		case string(CHAR_LTHAN), string(CHAR_GTHAN), "<=", ">=", "==", "!=":
			addToTokenList(sym, TOKEN_TYPE_BINOP, &tokenList)
		case string(CHAR_EQUALS):
			addToTokenList(sym, TOKEN_TYPE_ASSIGNEMENT, &tokenList)
		case string(CHAR_LPAREN):
			addToTokenList(sym, TOKEN_TYPE_LPAREN, &tokenList)
		case string(CHAR_RPAREN):
			addToTokenList(sym, TOKEN_TYPE_RPAREN, &tokenList)
		case string(CHAR_COMMA):
			addToTokenList(sym, TOKEN_TYPE_COMMA, &tokenList)

		default:
			addToTokenList(sym, TOKEN_TYPE_SYM, &tokenList)
		}

	}
	return tokenList
}

func collectSym(ptr *int, content []byte) []byte {
	if *ptr >= len(content) {
		return []byte{}
	}
	for content[*ptr] == CHAR_WSP {
		(*ptr)++
	}

	switch content[*ptr] {
	case CHAR_LPAREN, CHAR_RPAREN, CHAR_SEMICOLON, CHAR_PLUS, CHAR_MINUS, CHAR_COMMA, CHAR_SQUOTE, CHAR_QUOTE:
		*ptr++
		return []byte{content[(*ptr - 1)]}
	case CHAR_NOT, CHAR_LTHAN, CHAR_GTHAN:
		*ptr++
		if content[*ptr] == CHAR_EQUALS {
			*ptr++
			return []byte{content[(*ptr - 2)], content[*ptr-1]}
		} else {
			return []byte{content[(*ptr - 1)]}
		}
	case CHAR_EQUALS:
		*ptr++
		if *ptr < len(content) && content[*ptr] == CHAR_EQUALS {
			*ptr++
			return []byte{CHAR_EQUALS, CHAR_EQUALS}
		} else {
			return []byte{CHAR_EQUALS}
		}
	default:
		buf := []byte{}
		for tmpPtr := *ptr; tmpPtr < len(content) && isNormalSym(content[tmpPtr]); tmpPtr++ {
			buf = append(buf, content[tmpPtr])
			*ptr = tmpPtr
		}
		*ptr++
		return buf
	}
}

func isNormalSym(char byte) bool {
	return !contains(reservedSyms, char)
}

func addToTokenList(content []byte, tokenType TokenType, tokenList *[]JdbToken) {
	*tokenList = append((*tokenList), JdbToken{
		TokenType: tokenType,
		contents:  content,
	})
}
