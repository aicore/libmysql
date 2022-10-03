import {getColumNameForJsonField, isAlphaChar, isAlphaNumChar, isDigitChar} from "./sharedUtils.js";

// Token types
export const TOKEN_SPACE = ' ',
    TOKEN_BRACKET_OPEN = '(',
    TOKEN_BRACKET_CLOSE = ')',
    TOKEN_NUMBER = '1',
    TOKEN_VARIABLE = '#',
    // operators start
    TOKEN_OP_PLUS = '+',
    TOKEN_OP_MINUS = '-',
    TOKEN_OP_MUL = '*',
    TOKEN_OP_DIV = '/',
    TOKEN_OP_MOD = '%',
    TOKEN_OP_EQ = '=',
    TOKEN_OP_NOT = '!',
    TOKEN_OP_NOT_EQ = '!=',
    // operators end
    TOKEN_SINGLE_QUOTE_STRING = "'"; // a full string of the form 'hello \'world' with escape char awareness

export const OPERATOR_TOKENS =[TOKEN_OP_PLUS, TOKEN_OP_MINUS, TOKEN_OP_MUL, TOKEN_OP_DIV, TOKEN_OP_MOD, TOKEN_OP_EQ,
    TOKEN_OP_NOT, TOKEN_OP_NOT_EQ];

function _createToken(type, tokenStr) {
    return {
        type: type,
        str: tokenStr
    };
}

/**
 * Retrieves the space token at current position
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - TOKEN_SPACE
 * str - Will return a single char space string or multiple char string as encountered.
 * @private
 */
function _getSpaceToken(tokenizer) {
    let i = tokenizer.currentIndex,
        queryChars = tokenizer.queryChars;
    let tokenStr = "";
    while(i < queryChars.length && queryChars[i] === TOKEN_SPACE){
        tokenStr += ' ';
        i++;
    }
    tokenizer.currentIndex = i;
    return _createToken(TOKEN_SPACE, tokenStr);
}

/**
 * Retrieves the operator token at current position
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - One of the operator types defined in OPERATOR_TOKENS
 * str - The actual operator token string.
 * @private
 */
function _getOperatorToken(tokenizer) {
    let i = tokenizer.currentIndex,
        queryChars = tokenizer.queryChars;
    let tokenStr = "";
    while(i < queryChars.length && OPERATOR_TOKENS.includes(queryChars[i])){
        tokenStr += queryChars[i];
        i++;
    }
    tokenizer.currentIndex = i;
    if(!OPERATOR_TOKENS.includes(tokenStr)){
        throw new Error(`Unexpected Operator Token ${tokenStr} in query ${tokenizer.queryChars.join("")}`);
    }
    // at this point, the tokenStr is the same as token constant for allowed operators.
    return _createToken(tokenStr, tokenStr);
}

/**
 * Retrieves the single quoted string token at current position of the format: 'hello \'world' with escape
 * char awareness
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - TOKEN_SPACE
 * str - Will return the full string token of form 'hello world' including the single quotes.
 * @private
 */
function _getStringToken(tokenizer) {
    let i = tokenizer.currentIndex,
        queryChars = tokenizer.queryChars;
    let tokenStr = TOKEN_SINGLE_QUOTE_STRING, prevEscapeChar = false;
    i++; // move ptr to after single quote '^here
    while(i < queryChars.length && (prevEscapeChar || queryChars[i] !== TOKEN_SINGLE_QUOTE_STRING)){
        prevEscapeChar = (queryChars[i] === '\\');
        tokenStr += queryChars[i];
        i++;
    }
    if(i < queryChars.length && !prevEscapeChar && queryChars[i] === TOKEN_SINGLE_QUOTE_STRING){
        // add the ending quote only if it is present in the source string.
        tokenStr += "'";
        i++;
    }
    tokenizer.currentIndex = i;
    return _createToken(TOKEN_SINGLE_QUOTE_STRING, tokenStr);
}

/**
 * Retrieves the variable token at current position of the form x or x.y or x.y.z etc..
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - TOKEN_SPACE
 * str - Will return a variable token at current position of the form x or x.y or x.y.z etc..
 * @private
 */
function _getVariableToken(tokenizer) {
    let i = tokenizer.currentIndex,
        queryChars = tokenizer.queryChars;
    let tokenStr = "";
    while(i < queryChars.length && (isAlphaNumChar(queryChars[i]) || queryChars[i] === '.' || queryChars[i] === '_')){
        tokenStr += queryChars[i];
        i++;
    }
    tokenizer.currentIndex = i;
    return _createToken(TOKEN_VARIABLE, tokenStr);
}

/**
 * Retrieves the number token at current position of the form 0 or 0.0, or .0
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - TOKEN_NUMBER
 * str - Will return a number token of the form 0 or 0.0, or .0
 * @private
 */
function _getNumberToken(tokenizer) {
    let i = tokenizer.currentIndex,
        queryChars = tokenizer.queryChars;
    let tokenStr = "";
    while(i < queryChars.length && (isDigitChar(queryChars[i]) || queryChars[i] === '.')){
        tokenStr += queryChars[i];
        i++;
    }
    tokenizer.currentIndex = i;
    if((tokenStr.match(/\./g) || []).length > 1 || tokenStr.endsWith(".")){
        // number should have at most one `.`, error out for 1.2.3
        throw new Error(`Unexpected Number Token ${tokenStr} in query ${tokenizer.queryChars.join("")}`);
    }
    return _createToken(TOKEN_NUMBER, tokenStr);
}

/**
 * Retrieves the next token or null if there are no further tokens.
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null} An object that contains token type: one of constants TOKEN_* and the
 * actual string token encountered. If End of string, null is returned.
 * @private
 */
function nextToken(tokenizer) {
    if(tokenizer.currentIndex >= tokenizer.queryChars.length){
        return null; // end of string, no more tokens
    }
    let tokenStartChar = tokenizer.queryChars[tokenizer.currentIndex];
    switch (tokenStartChar) {
    case '"': throw new Error(`Strings Should Be in single quotes(Eg 'str') in query ${tokenizer.queryChars.join("")}`);
    case TOKEN_SPACE: return _getSpaceToken(tokenizer);
    case TOKEN_SINGLE_QUOTE_STRING: return _getStringToken(tokenizer);
    case TOKEN_BRACKET_OPEN: tokenizer.currentIndex++;
        return _createToken(TOKEN_BRACKET_OPEN, tokenStartChar);
    case TOKEN_BRACKET_CLOSE: tokenizer.currentIndex++;
        return _createToken(TOKEN_BRACKET_CLOSE, tokenStartChar);
    default:
        if(tokenStartChar === '.' || isDigitChar(tokenStartChar)){
            // If the operator contains multiple characters, review this check. Currently only ! and != is considered
            return _getNumberToken(tokenizer);
        }
        if(OPERATOR_TOKENS.includes(tokenStartChar)){
            // If the operator contains multiple characters, review this check. Currently only ! and != is considered
            return _getOperatorToken(tokenizer);
        }
        if(isAlphaChar(tokenStartChar) || tokenStartChar ==='_'){
            return _getVariableToken(tokenizer);
        }
        throw new Error(`Unexpected Token char ${tokenStartChar} in query ${tokenizer.queryChars.join("")}`);
    }
}

function getTokenizer(queryString) {
    queryString = queryString || "";
    let queryChars = Array.from(queryString);
    return {
        queryChars,
        currentIndex: 0
    };
}

function _transformQuery(queryString) {
    let tokenizer = getTokenizer(queryString);
    return queryString;
}

function transformCocoToSQLQuery(indexQuery, nonIndexQuery) {
    indexQuery = _transformQuery(indexQuery);
    nonIndexQuery = _transformQuery(nonIndexQuery);
    if(indexQuery && nonIndexQuery){
        return indexQuery + " AND " + nonIndexQuery;
    }
    return indexQuery || nonIndexQuery;
}

export const QueryTokenizer = {
    getTokenizer,
    nextToken
};

const Query ={
    transformCocoToSQLQuery
};

export default Query;
