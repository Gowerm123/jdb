package jdbql

const (
	CHAR_LPAREN    = '('
	CHAR_RPAREN    = ')'
	CHAR_SEMICOLON = ';'
	CHAR_TAC       = '\''
	CHAR_PLUS      = '+'
	CHAR_MINUS     = '-'
	CHAR_NOT       = '!'
	CHAR_COMMA     = ','
	CHAR_QUOTE     = '"'
	CHAR_LTHAN     = '<'
	CHAR_GTHAN     = '>'
	CHAR_EQUALS    = '='
	CHAR_WSP       = ' '
)

var reservedSyms = []byte{CHAR_SEMICOLON, CHAR_TAC, CHAR_QUOTE, CHAR_PLUS, CHAR_MINUS, CHAR_NOT, CHAR_COMMA, CHAR_WSP, CHAR_EQUALS, CHAR_LPAREN, CHAR_RPAREN}

func Lex(content []byte) []JdbToken {
	return lex(content)
}

func lex(content []byte) []JdbToken {
	ptr := 0
	tokenList := []JdbToken{}
	for ptr < len(content) {
		sym := collectSym(&ptr, content)
		switch string(sym) {
		case ";":
			addToTokenList(sym, TOKEN_TYPE_SEMICOLON, &tokenList)
		case "\"":
			addToTokenList(sym, TOKEN_TYPE_QUOTE, &tokenList)
		case "'":
			addToTokenList(sym, TOKEN_TYPE_TIC, &tokenList)
		case "+", "-":
			addToTokenList(sym, TOKEN_TYPE_BINOP, &tokenList)
		case "<", ">", "<=", ">=", "==", "!=":
			addToTokenList(sym, TOKEN_TYPE_BINOP, &tokenList)
		case "=":
			addToTokenList(sym, TOKEN_TYPE_ASSIGNEMENT, &tokenList)
		case "(":
			addToTokenList(sym, TOKEN_TYPE_LPAREN, &tokenList)
		case ")":
			addToTokenList(sym, TOKEN_TYPE_RPAREN, &tokenList)
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
	case CHAR_LPAREN, CHAR_RPAREN, CHAR_SEMICOLON, CHAR_PLUS, CHAR_MINUS, CHAR_COMMA, CHAR_TAC, CHAR_QUOTE:
		*ptr++
		return []byte{content[(*ptr - 1)]}
	case CHAR_NOT, CHAR_LTHAN, CHAR_GTHAN:
		*ptr++
		if content[*ptr] == CHAR_EQUALS {
			return []byte{content[(*ptr - 1)], content[*ptr]}
		} else {
			return []byte{content[(*ptr - 1)]}
		}
	default:
		buf := []byte{}
		for tmpPtr := *ptr; isNormalSym(content[tmpPtr]); tmpPtr++ {
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
