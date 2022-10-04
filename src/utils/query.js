import {getColumNameForJsonField, isAlphaChar, isAlphaNumChar, isDigitChar} from "./sharedUtils.js";
import {JSON_COLUMN} from "./constants.js";
// Token types
export const TOKEN_SPACE = ' ',
    TOKEN_BRACKET_OPEN = '(',
    TOKEN_BRACKET_CLOSE = ')',
    TOKEN_NUMBER = '1',
    TOKEN_VARIABLE = '#',
    TOKEN_FUNCTION = 'fn',
    TOKEN_KEYWORD = 'key',
    TOKEN_DOCUMENT = '$',
    // operators start
    TOKEN_OP_COMMA = ',',
    TOKEN_OP_PLUS = '+',
    TOKEN_OP_MINUS = '-',
    TOKEN_OP_MUL = '*',
    TOKEN_OP_DIV = '/',
    TOKEN_OP_MOD = '%',
    TOKEN_OP_EQ = '=',
    TOKEN_OP_NOT = '!',
    TOKEN_OP_NOT_EQ = '!=',
    TOKEN_OP_GT = '>',
    TOKEN_OP_GT_EQ = '>=',
    TOKEN_OP_LT = '<',
    TOKEN_OP_LT_EQ = '<=',
    TOKEN_OP_BIT_AND = '&',
    TOKEN_OP_AND = '&&',
    TOKEN_OP_BIT_OR = '|',
    TOKEN_OP_OR = '||',
    TOKEN_OP_BITWISE_INVERT = '~',
    // operators end
    TOKEN_SINGLE_QUOTE_STRING = "'"; // a full string of the form 'hello \'world' with escape char awareness

// https://dev.mysql.com/doc/refman/8.0/en/non-typed-operators.html
export const OPERATOR_TOKENS =[TOKEN_OP_PLUS, TOKEN_OP_MINUS, TOKEN_OP_MUL, TOKEN_OP_DIV, TOKEN_OP_MOD, TOKEN_OP_EQ,
    TOKEN_OP_NOT, TOKEN_OP_NOT_EQ, TOKEN_OP_GT, TOKEN_OP_GT_EQ, TOKEN_OP_LT, TOKEN_OP_LT_EQ, TOKEN_OP_COMMA,
    TOKEN_OP_BIT_AND, TOKEN_OP_AND, TOKEN_OP_BIT_OR, TOKEN_OP_OR, TOKEN_OP_BITWISE_INVERT];

// please update docs of transformCocoToSQLQuery if you are changing the below supported functions/keywords
export const MYSQL_FUNCTIONS =[
    // MATH functions defined in https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html
    'ABS', 'ACOS', 'ASIN', 'ATAN', 'ATAN2', 'ATAN', 'CEIL', 'CEILING', 'CONV', 'COS', 'COT',
    'CRC32', 'DEGREES', 'EXP', 'FLOOR', 'LN', 'LOG', 'LOG10', 'LOG2', 'MOD', 'PI', 'POW', 'POWER', 'RADIANS', 'RAND',
    'ROUND', 'SIGN', 'SIN', 'SQRT', 'TAN', 'TRUNCATE',
    // String functions defined in https://dev.mysql.com/doc/refman/8.0/en/string-functions.html
    "ASCII", "BIN", "BIT_LENGTH", "CHAR", "CHAR_LENGTH", "CHARACTER_LENGTH", "CONCAT", "CONCAT_WS", "ELT", "EXPORT_SET",
    "FIELD", "FORMAT", "FROM_BASE64", "HEX", "INSERT", "INSTR", "LCASE", "LEFT", "LENGTH", "LOAD_FILE", "LOCATE",
    "LOWER", "LPAD", "LTRIM", "MAKE_SET", "MATCH", "MID", "OCT", "OCTET_LENGTH", "ORD", "POSITION", "QUOTE",
    "REGEXP_INSTR", "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR", "REPEAT", "REPLACE", "REVERSE", "RIGHT",
    "RPAD", "RTRIM", "SOUNDEX", "SPACE", "SUBSTR", "SUBSTRING", "SUBSTRING_INDEX", "TO_BASE64", "TRIM",
    "UCASE", "UNHEX", "UPPER", "WEIGHT_STRING",
    // comparison
    "SOUNDS", "STRCMP",
    // Selected APIs defined in https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html
    "IF", "IFNULL", "NULLIF", "IN",
    // Selected JSON Functions in https://dev.mysql.com/doc/refman/8.0/en/json-function-reference.html
    "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_KEYS", "JSON_DEPTH", "JSON_LENGTH", "JSON_REMOVE", "JSON_REPLACE",
    "JSON_INSERT", "JSON_MERGE_PATCH", "JSON_MERGE_PRESERVE", "JSON_OVERLAPS", "JSON_SEARCH", "JSON_SET", "JSON_TYPE",
    "JSON_VALID", "JSON_VALUE", "MEMBER", "OF" // "MEMBER OF" is a single keyword, but tokenized as two
];

export const MYSQL_KEYWORDS =[
    "LIKE", "NOT", "REGEXP", "RLIKE", "NULL", "AND", "OR", "IS", "BETWEEN", "XOR"
];

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
 * Retrieves the variable or function token at current position
 * * variable token is of the form x or x.y or x.y.z etc..
 * * function token is one of the predefined constants in MYSQL_FUNCTIONS
 * @param {{queryChars: Array, currentIndex: number}} tokenizer
 * @return {{type: string, str: string} | null}
 * type - TOKEN_SPACE
 * str - Will return a variable token at current position of the form x or x.y or x.y.z etc..
 * @private
 */
function _getVariableOrFuncToken(tokenizer) {
    let i = tokenizer.currentIndex,
        queryChars = tokenizer.queryChars;
    let tokenStr = "";
    while(i < queryChars.length && (isAlphaNumChar(queryChars[i]) || queryChars[i] === '.' || queryChars[i] === '_')){
        tokenStr += queryChars[i];
        i++;
    }
    tokenizer.currentIndex = i;
    if(MYSQL_FUNCTIONS.includes(tokenStr)){
        return _createToken(TOKEN_FUNCTION, tokenStr);
    }
    if(MYSQL_KEYWORDS.includes(tokenStr)){
        return _createToken(TOKEN_KEYWORD, tokenStr);
    }
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
    case TOKEN_DOCUMENT: tokenizer.currentIndex++;
        return _createToken(TOKEN_DOCUMENT, tokenStartChar);
    case TOKEN_BRACKET_CLOSE: tokenizer.currentIndex++;
        return _createToken(TOKEN_BRACKET_CLOSE, tokenStartChar);
    default:
        if(tokenStartChar === '.' || isDigitChar(tokenStartChar)){
            return _getNumberToken(tokenizer);
        }
        if(OPERATOR_TOKENS.includes(tokenStartChar)){
            // If the operator contains multiple characters, review this check. For Eg., `!=` will only reach here if
            // `!` is in the OPERATOR_TOKENS array.
            return _getOperatorToken(tokenizer);
        }
        if(isAlphaChar(tokenStartChar) || tokenStartChar ==='_'){
            return _getVariableOrFuncToken(tokenizer);
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

/**
 * Transforms CocoDB queries to MYSql queries. Coco queries closely resemble the mysql query syntax. The json field
 * names can be directly specified in coco queries.
 * A Sample coco query:
 * `NOT(customerID = 35 && (price.tax < 18 OR ROUND(price.amount) != 69))`
 *
 * ## `$` is a special character that denotes the JSON document itself.
 * It can be used for json compare as. `JSON_CONTAINS($,'{"name": "v"}')`.
 * ** WARNING: JSON_CONTAINS this will not use the index. We may add support in future, but not presently. **
 *
 * ## cocodb query syntax
 * cocodb query syntax closely resembles mysql query syntax. The following functions are supported as is:
 *
 * ### Supported functions
 * #### MATH functions defined in https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html
 *     'ABS', 'ACOS', 'ASIN', 'ATAN', 'ATAN2', 'ATAN', 'CEIL', 'CEILING', 'CONV', 'COS', 'COT',
 *     'CRC32', 'DEGREES', 'EXP', 'FLOOR', 'LN', 'LOG', 'LOG10', 'LOG2', 'MOD', 'PI', 'POW', 'POWER', 'RADIANS', 'RAND',
 *     'ROUND', 'SIGN', 'SIN', 'SQRT', 'TAN', 'TRUNCATE',
 * #### String functions defined in https://dev.mysql.com/doc/refman/8.0/en/string-functions.html
 *     "ASCII", "BIN", "BIT_LENGTH", "CHAR", "CHAR_LENGTH", "CHARACTER_LENGTH", "CONCAT", "CONCAT_WS", "ELT", "EXPORT_SET",
 *     "FIELD", "FORMAT", "FROM_BASE64", "HEX", "INSERT", "INSTR", "LCASE", "LEFT", "LENGTH", "LOAD_FILE", "LOCATE",
 *     "LOWER", "LPAD", "LTRIM", "MAKE_SET", "MATCH", "MID", "OCT", "OCTET_LENGTH", "ORD", "POSITION", "QUOTE",
 *     "REGEXP_INSTR", "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR", "REPEAT", "REPLACE", "REVERSE", "RIGHT",
 *     "RPAD", "RTRIM", "SOUNDEX", "SPACE", "SUBSTR", "SUBSTRING", "SUBSTRING_INDEX", "TO_BASE64", "TRIM",
 *     "UCASE", "UNHEX", "UPPER", "WEIGHT_STRING",
 * #### comparison
 *     "SOUNDS", "STRCMP",
 * #### Selected APIs defined in https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html
 *     "IF", "IFNULL", "NULLIF", "IN",
 * #### Selected JSON Functions in https://dev.mysql.com/doc/refman/8.0/en/json-function-reference.html
 *     "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_KEYS", "JSON_DEPTH", "JSON_LENGTH", "JSON_REMOVE", "JSON_REPLACE",
 *     "JSON_INSERT", "JSON_MERGE_PATCH", "JSON_MERGE_PRESERVE", "JSON_OVERLAPS", "JSON_SEARCH", "JSON_SET", "JSON_TYPE",
 *     "JSON_VALID", "JSON_VALUE", "MEMBEROF"
 * #### Other Keywords
 *     "LIKE", "NOT", "REGEXP", "RLIKE", "NULL", "AND", "OR", "IS", "BETWEEN", "XOR"
 *
 * @param {string} query The query as string.
 * @param {Array<String>} useIndexForFields A string array of field names for which the index should be used. Note
 * that an index should first be created using `createIndexForJsonField` API. Eg. ['customerID', 'price.tax']
 * @return {string} The MYSQL query string corresponding to the cocdb query.
 */
function transformCocoToSQLQuery(query, useIndexForFields = []) {
    if(!Array.isArray(useIndexForFields)){
        throw new Error(`invalid argument: useIndexForFields should be an array`);
    }
    let tokenizer = getTokenizer(query),
        sqlQuery = '';
    let token = nextToken(tokenizer);
    while(token) {
        if(token.type === TOKEN_VARIABLE) {
            let variable = token.str;
            if(useIndexForFields.includes(variable)){
                sqlQuery += getColumNameForJsonField(variable);
            } else {
                sqlQuery += `${JSON_COLUMN}->>"$.${variable}"`;
            }
        } else if(token.type === TOKEN_DOCUMENT) {
            // The $ char is alias to json document itself in query
            sqlQuery += `${JSON_COLUMN}`;
        } else {
            sqlQuery += token.str;
        }
        token = nextToken(tokenizer);
    }
    return sqlQuery;
}

export const QueryTokenizer = {
    getTokenizer,
    nextToken
};

const Query ={
    transformCocoToSQLQuery
};

export default Query;
