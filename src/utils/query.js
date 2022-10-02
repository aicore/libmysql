import {getColumNameForJsonField, isAlphaChar, isAlphaNumChar} from "./sharedUtils.js";

// Token types
const TOKEN_SPACE = ' ',
    TOKEN_BRACKET_OPEN = '(',
    TOKEN_BRACKET_CLOSE = ')',
    TOKEN_VARIABLE = '#',
    TOKEN_SINGLE_QUOTE_STRING = "'"; // a full string of the form 'hello \'world' with escape char awareness

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
 * Retrieves the single quoted string token at current position of the format: 'hello \'world' with escape
 * char awareness
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - TOKEN_SPACE
 * str - Will return a single char space string or multiple char string as encountered.
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
 * Retrieves the variable token at current position of the for x or x.y or x.y.z etc..
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - TOKEN_SPACE
 * str - Will return a single char space string or multiple char string as encountered.
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
 * Retrieves the next token or null if there are no further tokens.
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null} The following token types are read
 * space - Will return a single char space string or multiple char string as encountered.
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
