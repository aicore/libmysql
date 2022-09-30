import {getColumNameForJsonField} from "./sharedUtils.js";

// Token types
const TOKEN_SPACE = 1;

/**
 * Retrieves the space token at current position
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: number, str: string} | null}
 * type - TOKEN_SPACE
 * str - Will return a single char space string or multiple char string as encountered.
 * @private
 */
function _getSpaceToken(tokenizer) {
    let i = tokenizer.currentIndex,
        queryChars = tokenizer.queryChars;
    let token = "";
    while(i < queryChars.length && queryChars[i] === ' '){
        token += ' ';
        i++;
    }
    tokenizer.currentIndex = i;
    return {
        type: TOKEN_SPACE,
        str: token
    };
}

/**
 * Retrieves the next token or null if there are no further tokens.
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: number, str: string} | null} The following token types are read
 * space - Will return a single char space string or multiple char string as encountered.
 * @private
 */
function nextToken(tokenizer) {
    if(tokenizer.currentIndex >= tokenizer.queryChars.length){
        return null; // end of string, no more tokens
    }
    let nextTokenChar = tokenizer.queryChars[tokenizer.currentIndex];
    switch (nextTokenChar) {
    case " ": return _getSpaceToken(tokenizer);
    default: throw new Error(`Unexpected Token char ${nextTokenChar} in query ${tokenizer.queryChars.join("")}`);
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
