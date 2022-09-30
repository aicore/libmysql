import {getColumNameForJsonField} from "./sharedUtils.js";

// Token types
const TOKEN_SPACE = ' ',
    TOKEN_BRACKET_OPEN = '(',
    TOKEN_BRACKET_CLOSE = ')',
    TOKEN_SINGLE_QUOTE_STRING = '\''; // a full string of the form 'hello \'world' with escape char awareness

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
    while(i < queryChars.length && queryChars[i] === ' '){
        tokenStr += ' ';
        i++;
    }
    tokenizer.currentIndex = i;
    return _createToken(TOKEN_SPACE, tokenStr);
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
    case TOKEN_BRACKET_OPEN: tokenizer.currentIndex++;
        return _createToken(TOKEN_BRACKET_OPEN, tokenStartChar);
    case TOKEN_BRACKET_CLOSE: tokenizer.currentIndex++;
        return _createToken(TOKEN_BRACKET_CLOSE, tokenStartChar);
    default: throw new Error(`Unexpected Token char ${tokenStartChar} in query ${tokenizer.queryChars.join("")}`);
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
