import mysql from "mysql2";
import {isObject, isObjectEmpty, isString, isStringEmpty} from "@aicore/libcommonutils";
import crypto from "crypto";
import {isNumber} from "@aicore/libcommonutils/src/utils/common.js";
import {getColumNameForJsonField, isVariableNameLike, isNestedVariableNameLike} from "./sharedUtils.js";
import Query from './query.js';
import {
    JSON_COLUMN, MAX_NUMBER_OF_DOCS_ALLOWED, PRIMARY_COLUMN, DATA_TYPES,
    MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME, MAXIMUM_LENGTH_OF_MYSQL_DATABASE_NAME
} from './constants.js';

// @INCLUDE_IN_API_DOCS

let CONNECTION = null;
let LOGGER = console; // Default fallback for backward compatibility

/* Defining a constant variable called SIZE_OF_PRIMARY_KEY and assigning it the value of 32. */
const SIZE_OF_PRIMARY_KEY = 32;

/**
 * Gets the limit string for sql query pr the default limit 1000 string
 * @param {Object} options
 * @param {number} options.pageOffset specify which row to start retrieving documents from. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @param {number} options.pageLimit specify number of documents to retrieve. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @private
 */
function _getLimitString(options) {
    if(!options.pageLimit && !options.pageOffset){
        // default limit
        return `LIMIT ${MAX_NUMBER_OF_DOCS_ALLOWED}`;
    }
    if(!isNumber(options.pageOffset) || !isNumber(options.pageLimit)){
        throw new Error(`Expected required options options.pageOffset and options.pageLimit as numbers but got `
            + typeof options.pageOffset + " and " + typeof options.pageLimit);
    }
    if(options.pageLimit > 1000){
        throw new Error("options.pageLimit Cannot exceed 1000");
    }
    return `LIMIT ${options.pageOffset}, ${options.pageLimit}`;
}

/**
 * It creates a database with the name provided as an argument
 * @param {string} databaseName - The name of the database to create.
 * @returns {Promise<boolean>}- A promise which helps to know if createDataBase is successful
 */
export function createDataBase(databaseName) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before createDataBase');
            return;
        }
        if (!_isValidDatBaseName(databaseName)) {
            reject('Please provide valid data base name');
            return;
        }

        try {
            const createDataBaseSql = `CREATE DATABASE ${databaseName}`;
            CONNECTION.execute(createDataBaseSql,
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        LOGGER.error({err, databaseName, operation: 'createDataBase'}, 'Error creating database');
                        reject(err);
                        return;
                    }
                    resolve(true);

                });
        } catch (e) {
            const errorMessage = `exception occurred while creating database ${e.stack}`;
            reject(errorMessage);
            //Todo: Emit metrics
        }
    });
}

/**
 * It deletes a database with the given name
 * @param {string} databaseName - The name of the database to be created.
 * @returns {Promise<boolean>} A promise which helps to know if database delete is successful
 */
export function deleteDataBase(databaseName) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before deleteDataBase');
            return;
        }
        if (!_isValidDatBaseName(databaseName)) {
            reject('Please provide valid data base name');
            return;
        }
        try {
            const createDataBaseSql = `DROP DATABASE ${databaseName}`;
            CONNECTION.execute(createDataBaseSql,
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        LOGGER.error({err, databaseName, operation: 'deleteDataBase'}, 'Error deleting database');
                        reject(err);
                        return;
                    }
                    resolve(true);

                });
        } catch (e) {
            const errorMessage = `execution occurred while deleting database ${e.stack}`;
            reject(errorMessage);
            //Todo: Emit metrics
        }

    });

}

function _isValidDatBaseName(databaseName) {
    return (isString(databaseName) && databaseName.length <=
        MAXIMUM_LENGTH_OF_MYSQL_DATABASE_NAME && isVariableNameLike(databaseName));
}

/** This function helps to initialize MySql Client
 * This function should be called before calling any other functions in this library
 *
 * Best practice is to `import @aicore/libcommonutils` and call `getMySqlConfigs()` api to read values from of configs
 * from environment variables.
 * @example <caption> Sample config </caption>
 *
 *  const config = {
 *    "host": "localhost",
 *    "port": "3306",
 *    "user" : "root",
 *    "password": "1234"
 *  };
 *
 * @example <caption> Sample initialization code  </caption>
 *
 * // set  following  environment variables to access database securely
 * // set MY_SQL_SERVER for mysql server
 * // set MY_SQL_SERVER_PORT to set server port
 * // set MY_SQL_USER to specify database user
 * // set MY_SQL_PASSWORD to set mysql password
 *
 * import {getMySqlConfigs} from "@aicore/libcommonutils";
 *
 * const configs = getMySqlConfigs();
 * init(configs)
 *
 *
 * @param {Object} config -  config to configure MySQL
 * @param {string} config.host - mysql database hostname
 * @param {string} config.port - port number of mysql db
 * @param {string} config.user - username of database
 * @param {string} config.password - password of database username
 * @param {Number} config.connectionLimit - Maximum MySql connection that can be open to the server default value is 10
 * @param {Object} logger - Logger instance for structured logging
 * @returns {boolean}  true if connection is successful false otherwise
 *
 *
 **/
export function init(config, logger) {
    // Store logger (fallback to console if not provided)
    LOGGER = logger || console;

    if (!isObject(config)) {
        throw new Error('Please provide valid config');
    }
    if (!isString(config.host)) {
        throw new Error('Please provide valid hostname');
    }
    if (!isString(config.port)) {
        throw new Error('Please provide valid port');
    }
    if (!isString(config.user)) {
        throw  new Error('Please provide valid user');
    }
    if (!isString(config.password)) {
        throw new Error('Please provide valid password');
    }
    if (CONNECTION) {
        LOGGER.warn({connection: 'already_active'}, 'Connection already active');
        throw  new Error('One connection is active please close it before reinitializing it');
    }
    try {
        config.waitForConnections = true;
        config.connectionLimit = !config.connectionLimit ? 10 : config.connectionLimit;
        config.queueLimit = 0;

        CONNECTION = mysql.createPool(config);
        return true;
    } catch (e) {
        LOGGER.error({err: e}, 'Failed to initialize MySQL connection');
        return false;
    }
}

/** This function helps to close the database connection
 * @return {void}
 *
 */
export function close() {
    if (!isObject(CONNECTION)) {
        return;
    }
    CONNECTION.end();
    CONNECTION = null;
}


/**
 * It checks if the nameSpace is a valid table name
 * @param {string} nameSpace - The name of the table.
 * @returns   {boolean}  A boolean value.
 * @private
 */
function _isValidTableName(nameSpace) {
    if (!nameSpace || !isString(nameSpace)) {
        return false;
    }
    const split = nameSpace.split('.');
    if (!split || split.length !== 2) {
        return false;
    }
    const tableName = split[1];
    return (isString(tableName) && tableName.length <=
        MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME && isVariableNameLike(tableName));
}

/**
 * Returns true if the given key is a string of length greater than zero and less than or equal to the maximum size of a
 * primary key.
 * @param {string} key - The primary key of the item to be retrieved.
 * @returns {boolean} A boolean value.
 * @private
 */
function _isValidPrimaryKey(key) {
    return isString(key) && key.length > 0 && key.length <= SIZE_OF_PRIMARY_KEY;
}

/** It creates a table in the database with the name provided as the parameter
 *
 * we have simplified our database schema, for us, our database has only two columns
 *  1. `primary key` column, which is a varchar(255)
 *  2. `JSON` column, which stores values corresponding to the primary key as `JSON`
 * using this approach will simplify our database design by delegating the handling of the semantics of
 * data to the application.To speed up any query, we have provided an option to add a secondary index
 * for JSON fields using `createIndexForJsonField` api.
 *
 * @example <caption> How to use this function? </caption>
 *
 * import {getMySqlConfigs} from "@aicore/libcommonutils";
 *
 * const configs = getMySqlConfigs();
 * init(configs)
 * const tableName = 'customer';
 * try {
 *   await createTable(tableName);
 * } catch(e){
 *     console.error(JSON.stringify(e));
 * }
 *
 *
 * @param {string} tableName  name of table to create
 * @return {Promise}  returns a `Promise` await on `Promise` to get status of `createTable`
 * `on success` await will return `true`. `on failure` await will throw an `exception`.
 *
 */
export function createTable(tableName) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before createTable');
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name in database.tableName format');
            return;
            //Todo: Emit metrics
        }
        try {
            const createTableSql = `CREATE TABLE ${tableName} 
                                        (${PRIMARY_COLUMN} VARCHAR(${SIZE_OF_PRIMARY_KEY}) NOT NULL PRIMARY KEY, 
                                        ${JSON_COLUMN} JSON NOT NULL)`;
            CONNECTION.execute(createTableSql,
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        LOGGER.error({err, tableName, operation: 'createTable'}, 'Error creating table');
                        reject(err);
                        return;
                    }
                    resolve(true);

                });
        } catch (e) {
            const errorMessage = `execution occurred while creating table ${e.stack}`;
            reject(errorMessage);
            //Todo: Emit metrics
        }
    });
}


/**
 * It takes a table name and a document and then inserts the document into the database.
 * @example <caption> Sample code </caption>
 *
 * try {
 *       const primaryKey = 'bob';
 *       const tableName = 'customers;
 *       const document = {
 *           'lastName': 'Alice',
 *           'Age': 100,
 *           'active': true
 *       };
 *       await put(tableName, document);
 *   } catch (e) {
 *       console.error(JSON.stringify(e));
 *  }
 *
 * @param {string} tableName - The name of the table in which you want to store the data.
 * @param {Object} document - The JSON string that you want to store in the database.
 * @returns {Promise} A promise on resolving the promise will give documentID throws an exception
 * otherwise. DocumentId is an alphanumeric string of length 128
 */
export function put(tableName, document) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before put');
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name in database.tableName format');
            return;
            //Todo: Emit metrics
        }

        if (!isObject(document)) {
            reject('Please provide valid document');
            return;
            //Todo: Emit metrics
        }

        const updateQuery = `INSERT INTO ${tableName} (${PRIMARY_COLUMN}, ${JSON_COLUMN}) values(?,?)`;
        try {
            const documentID = createDocumentId();
            CONNECTION.execute(updateQuery, [documentID, document],
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        LOGGER.error({err, tableName, operation: 'put'}, 'Error putting document');
                        reject(err);
                        return;
                    }
                    resolve(documentID);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while writing to database ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}


/**
 * It generates a random string of 16 hexadecimal characters
 * When converting hexadecimal to string. The generated string will contain 32 characters
 * @returns {string} A random string of hexadecimal characters.
 * @private
 */
function createDocumentId() {
    return crypto.randomBytes(16).toString('hex');
}

/**
 * It deletes a document from the database based on the document id. Conditional deletes are also supported
 * with the optional condition parameter.
 * @example <caption> Sample code </caption>
 *
 * const tableName = 'customers';
 * const documentID = '123456';
 * try {
 *    await deleteKey(tableName, documentID);
 * } catch(e) {
 *    console.error(JSON.stringify(e));
 * }
 *
 * @example <caption> Sample code with conditional option</caption>
 *
 * const tableName = 'customers';
 * const documentID = '123456';
 * try {
 *    // Eg. delete the document only if the last modified is equals 21
 *    await deleteKey(tableName, documentID, "$.lastModified=21");
 * } catch(e) {
 *    console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table in which the key is to be deleted.
 * @param {string} documentID -  document id to be deleted
 * @param {string} [condition] - Optional coco query condition of the form "$.cost<35" that must be satisfied
 * for delete to happen. See query API for more details on how to write coco query strings.
 * @returns {Promise}A promise `resolve` promise to get status of delete. promise will resolve to true
 * for success and  throws an exception for failure.
 */
export function deleteKey(tableName, documentID, condition) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before deleteKey');
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(documentID)) {
            reject('Please provide valid documentID');
            return;
            //Todo: Emit metrics
        }

        let deleteQuery = `DELETE FROM ${tableName} WHERE ${PRIMARY_COLUMN}= ?;`;
        if(condition){
            const sqlCondition = Query.transformCocoToSQLQuery(condition, []);
            deleteQuery = `DELETE FROM ${tableName} WHERE ${PRIMARY_COLUMN}= ? AND (${sqlCondition});`;
        }
        try {
            CONNECTION.execute(deleteQuery, [documentID],
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        LOGGER.error({err, tableName, documentID, operation: 'deleteKey'}, 'Error deleting key');
                        reject(err);
                        return;
                    }
                    resolve(true);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while deleting key ${documentID}  from database ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * Deletes a document from the database based the given query condition and returns the number of documents deleted.
 * @example <caption> Sample code </caption>
 *
 * const tableName = 'dbName:customers';
 * try {
 *    // delete all documents with field 'location.city' set to paris
 *    let deletedDocumentCount = await deleteDocuments(tableName, "$.location.city  = 'paris'", ['location.city']);
 * } catch(e) {
 *    console.error(e);
 * }
 *
 * @param {string} tableName - The name of the table in which the key is to be deleted.
 * @param {string} queryString - The cocDB query string.
 * @param {Array[string]} useIndexForFields - List of indexed fields in the document.
 * @returns {Promise} A promise `resolve` with the number of deleted documents or throws an exception for failure.
 */
export function deleteDocuments(tableName, queryString, useIndexForFields = []) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before deleteDocuments');
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!isString(queryString) || isStringEmpty(queryString)) {
            reject(`please provide valid queryString`);
            return;
        }
        let queryParseDone = false;
        try {
            let sqlQuery = Query.transformCocoToSQLQuery(queryString, useIndexForFields);
            queryParseDone = true;
            const deleteQuery = `DELETE FROM ${tableName} WHERE ${sqlQuery};`;
            CONNECTION.execute(deleteQuery,
                function (err, results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        LOGGER.error({err, tableName, operation: 'deleteDocuments'}, 'Error deleting documents');
                        reject(err);
                        return;
                    }
                    resolve(results.affectedRows);
                });
        } catch (e) {
            if (!queryParseDone) {
                reject(e.message); // return helpful error messages from query parser
                return;
            }
            reject("Exception occurred while deleting");
            //TODO: Emit Metrics
        }
    });
}

/**
 * It takes in a table name and documentId, and returns a promise that resolves to the document
 * @example <caption> sample code </caption>
 * const tableName = 'customers';
 * const documentID = '12345';
 * try {
 *     const results = await get(tableName, documentID);
 *     console.log(JSON.stringify(result));
 * } catch(e){
 *     console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table in which the data is stored.
 * @param {string} documentID - The primary key of the row you want to get.
 * @returns {Promise} A promise on resolve promise to get the value stored for documentID
 */
export function get(tableName, documentID) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before get');
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(documentID)) {
            reject('Please provide valid documentID');
            return;
            //Todo: Emit metrics
        }
        try {
            const getQuery = `SELECT ${PRIMARY_COLUMN},${JSON_COLUMN} FROM ${tableName} WHERE ${PRIMARY_COLUMN} = ?`;
            CONNECTION.execute(getQuery, [documentID],
                function (err, results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    if (results && results.length > 0) {
                        results[0][JSON_COLUMN].documentId = results[0][PRIMARY_COLUMN];
                        resolve(results[0][JSON_COLUMN]);
                        return;
                    }
                    reject('unable to find document for given documentId');
                });
        } catch (e) {
            const errorMessage = `Exception occurred while getting data ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * It takes a JSON object and returns a SQL query and an array of values to be used in a prepared statement
 * @param {Object} subQueryObject - This is the object that you want to query.
 * @param {string} [parentKey] - This is the parent key of the current object.
 * @returns {Object} An object with two properties: getQuery and valArray.
 * @private
 */
function _queryScanBuilder(subQueryObject, parentKey = "") {
    const valArray = [];
    let getQuery = '';
    let numberOfEntries = Object.keys(subQueryObject).length;
    for (const [key, value] of Object.entries(subQueryObject)) {
        if (isObject(value)) {
            let subResults = _queryScanBuilder(value, parentKey + "." + key);
            if (subResults) {
                getQuery += subResults.getQuery;
                subResults.valArray.forEach(result => {
                    valArray.push(result);
                });
                numberOfEntries = numberOfEntries - 1;
                continue;
            }
        }
        if (!isVariableNameLike(key)) {
            throw new Error(`Invalid filed name ${key}`);
        }
        if (numberOfEntries > 1) {
            getQuery = getQuery + `${JSON_COLUMN}->"$${parentKey}.${key}" = ? and `;

        } else {
            getQuery = getQuery + `${JSON_COLUMN}->"$${parentKey}.${key}" = ? `;
        }
        valArray.push(value);
        numberOfEntries = numberOfEntries - 1;
    }
    return {
        'getQuery': getQuery,
        'valArray': valArray
    };
}

/**
 * It takes a table name and a query object and returns a query string and an array of values to be used in a prepared
 * statement
 * @param {string} tableName - The name of the table to query.
 * @param {Object} queryObject - The query object that you want to run.
 * @param {Object} options Optional parameter to add pagination.
 * @param {number} options.pageOffset specify which row to start retrieving documents from. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @param {number} options.pageLimit specify number of documents to retrieve. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @returns {Object} An object with two properties: getQuery and valArray.
 * @private
 */
function _prepareQueryForScan(tableName, queryObject, options) {
    if (isObjectEmpty(queryObject)) {
        return {
            'getQuery': `SELECT ${PRIMARY_COLUMN},${JSON_COLUMN} FROM ${tableName} ${_getLimitString(options)}`,
            'valArray': []
        };
    }
    let getQuery = `SELECT ${PRIMARY_COLUMN},${JSON_COLUMN} FROM ${tableName} WHERE `;
    const subQuery = _queryScanBuilder(queryObject);
    return {
        'getQuery': getQuery + subQuery.getQuery + ` ${_getLimitString(options)}`,
        'valArray': subQuery.valArray
    };
}

/**
 * It takes a table name and a query object, and returns a promise that resolves to the
 * array of matched documents.
 * `NB: this api will not detect boolean fields while scanning`
 * This query is doing database scan. using this query frequently can degrade database performance. if this query
 * is more frequent consider creating index and use `getFromIndex` API
 * NB: This query will return only 1000 entries.
 * @example <caption> sample code </caption>
 * const tableName = 'customers';
 * const queryObject = {
 *             'lastName': 'Alice',
 *             'Age': 100
 *         };
 * try {
 *     const scanResults = await getFromNonIndex(tableName, queryObject);
 *     console.log(JSON.stringify(scanResults));
 * } catch (e){
 *     console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table you want to query.
 * @param {Object} queryObject - This is the object that you want to query.
 * @param {Object} options Optional parameter to add pagination.
 * @param {number} options.pageOffset specify which row to start retrieving documents from. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @param {number} options.pageLimit specify number of documents to retrieve. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @returns {Promise} - A promise; on promise resolution returns array of  matched documents. if there are
 * no match returns empty array
 */
export function getFromNonIndex(tableName, queryObject = {}, options = {}) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before getFromNonIndex');
            return;
        }
        if (!isObject(queryObject)) {
            reject(`please provide valid queryObject`);
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        let queryParseDone = false;
        try {
            const queryParams = _prepareQueryForScan(tableName, queryObject, options);
            queryParseDone = true;
            _queryIndex(queryParams, resolve, reject);
        } catch (e) {
            if (!queryParseDone) {
                reject(e.message); // return helpful error messages from query parser
                return;
            }
            const errorMessage = `Exception occurred while getting data`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * It deletes a table from the database
 * @example <caption> Sample code </caption>
 * const tableName = 'customer';
 * try{
 *   await deleteTable(tableName);
 * } catch(e){
 *     console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table to be deleted.
 * @returns {Promise} - A promise that will resolve to true if the table is deleted, or reject with an error
 * if the table is not deleted.
 */
export function deleteTable(tableName) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before getFromNonIndex');
            return;
        }

        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }

        try {
            const deleteTableQuery = `DROP TABLE IF EXISTS ${tableName} CASCADE`;
            CONNECTION.execute(deleteTableQuery,
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(true);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while getting data`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });

}


/**
 * _buildCreateJsonColumQuery(tableName, nameOfJsonColumn, jsonField, dataTypeOfNewColumn, isNotNull, isUnique)
 *
 * The function takes the following parameters:
 *
 * * tableName - the name of the table to add the column to
 * * nameOfJsonColumn - the name of the new column
 * * jsonField - the name of the field in the JSON column to extract
 * * dataTypeOfNewColumn - the data type of the new column
 * * isNotNull - a boolean value indicating whether the new column should be nullable
 * * isUnique - a boolean value indicating whether the new column should be unique
 *
 * @param {string} tableName - The name of the table you want to add the column to.
 * @param {string} nameOfJsonColumn - The name of the new column that will be created.
 * @param {string} jsonField - The field in the JSON object that you want to extract.
 * @param {string} dataTypeOfNewColumn - This is the data type of the new column.
 * @param {boolean} isNotNull - If the new column should be NOT NULL
 * @param {boolean} isUnique - If true, the new column will be a unique key.
 * @returns  {string} A string that is a query to add a column to a table.
 * @private
 *
 */
function _buildCreateJsonColumQuery(tableName, nameOfJsonColumn, jsonField,
    dataTypeOfNewColumn, isNotNull, isUnique) {

    return `ALTER TABLE ${tableName} ADD COLUMN ${nameOfJsonColumn} ${dataTypeOfNewColumn}  GENERATED ALWAYS` +
        ` AS (${JSON_COLUMN}->>"$.${jsonField}") ${isNotNull ? " NOT NULL" : ""} ${isUnique ? " UNIQUE KEY" : ""};`;
}

/**
 * It takes a table name, a json field name, and a boolean value indicating whether the index should be unique or not,
 * and returns a string containing the SQL query to create the index
 * @param {string} tableName - The name of the table to create the index on.
 * @param {string} jsonField - The name of the JSON field that you want to index.
 * @param {boolean} isUnique - If true, the index will be unique.
 * @returns {string} A string that is a query to create an index on a table.
 * @private
 */
function _buildCreateIndexQuery(tableName, jsonField, isUnique) {
    if (isUnique) {
        return `CREATE UNIQUE INDEX  idx_${jsonField} ON ${tableName}(${jsonField});`;
    }
    return `CREATE INDEX  idx_${jsonField} ON ${tableName}(${jsonField});`;
}

/**
 * It creates an index on the JSON field in the table
 * @param{function} resolve - A function that is called when the promise is resolved.
 * @param {function} reject - A function that will be called if the promise is rejected.
 * @param {string} tableName - The name of the table to create the index on
 * @param {string} jsonField - The JSON field that you want to create an index on.
 * @param {boolean} isUnique - true if the index is unique, false otherwise
 * @returns {void}
 * @private
 *
 *  NB `private function exporting this for testing`
 *
 */
export function _createIndex(resolve, reject, tableName, jsonField, isUnique) {

    try {
        const indexQuery = _buildCreateIndexQuery(tableName, jsonField, isUnique);
        CONNECTION.execute(indexQuery,
            function (err, _results, _fields) {
                //TODO: emit success or failure metrics based on return value
                if (err) {
                    reject(err);
                    return;
                }
                resolve(true);
            });
    } catch (e) {
        const errorMessage = `Exception occurred while creating index for JSON field`;
        reject(errorMessage);
        //TODO: Emit Metrics
    }
}

/**
 * It creates a new column in the table for the JSON field and then creates an index on that column.
 * `NB: this will not work with boolean fields`
 * @example <caption> Sample code </caption>
 * const tableName = 'customers';
 * let jsonfield = 'lastName';
 * // supported data types can be found on https://dev.mysql.com/doc/refman/8.0/en/data-types.html
 * let dataTypeOfNewColumn = 'VARCHAR(50)';
 * let isUnique = false;
 * try{
 *      await createIndexForJsonField(tableName jsonfield, dataTypeOfNewColumn, isUnique);
 *      jsonfield = 'Age';
 *      dataTypeOfNewColumn = 'INT';
 *      isUnique = false;
 *
 *      await createIndexForJsonField(tableName, nameOfJsonColumn, jsonfield, dataTypeOfNewColumn, isUnique);
 * } catch (e){
 *      console.error(JSON.stringify(e));
 * }
 * @param {string} tableName - The name of the table in which you want to create the index.
 * @param {string} jsonField - The name of the field in the JSON object that you want to index. The filed name
 * should be a valid variable name of the form "x" or "x.y.z".
 * @param {string} dataTypeOfNewColumn - This is the data type of the new column that will be created.
 * visit https://dev.mysql.com/doc/refman/8.0/en/data-types.html to know all supported data types
 * @param {boolean} isUnique - If true, the json filed has to be unique for creating index.
 * @param {boolean} isNotNull - If true, the column will be created with NOT NULL constraint.
 * @returns {Promise} A promise
 */
export function createIndexForJsonField(tableName, jsonField, dataTypeOfNewColumn, isUnique = false,
    isNotNull = false) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before createIndexForJsonField');
            return;
        }

        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!isNestedVariableNameLike(jsonField)) {
            reject('please provide valid name for json field');
            return;
            //Todo: Emit metrics
        }
        if (!isString(dataTypeOfNewColumn)) {
            reject('please provide valid  data type for json field');
            return;
        }
        const sqlJsonColumn = getColumNameForJsonField(jsonField);

        try {
            const createColumnQuery = _buildCreateJsonColumQuery(tableName,
                sqlJsonColumn,
                jsonField,
                dataTypeOfNewColumn, isNotNull, isUnique);
            CONNECTION.execute(createColumnQuery,
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    return _createIndex(resolve, reject, tableName, sqlJsonColumn, isUnique);
                });
        } catch (e) {
            LOGGER.error({err: e, tableName, jsonField, operation: 'createIndexForJsonField'}, 'Error creating index');
            const errorMessage = `Exception occurred while creating column for JSON field`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * It takes a nested object and returns a query string and an array of values
 * @param {Object} subQueryObject - This is the object that you want to convert to a query.
 * @param {string} [parentKey] - This is the key of the parent object.
 * @returns {Object} An object with two properties, getQuery and valArray.
 * @private
 */
function _prepareQueryForNestedObject(subQueryObject, parentKey = "") {
    const valArray = [];
    let subQuery = '';
    let numberOfEntries = Object.keys(subQueryObject).length;
    for (const [key, value] of Object.entries(subQueryObject)) {
        if (isObject(value)) {
            const partialKey = (parentKey === "") ? key : parentKey + '.' + key;
            let subResults = _prepareQueryForNestedObject(value, partialKey);
            if (subResults) {
                subQuery += subResults.getQuery;
                subResults.valArray.forEach(result => {
                    valArray.push(result);
                });
            }
            numberOfEntries = numberOfEntries - 1;
            continue;
        }
        const fullKey = (parentKey === "") ? key : parentKey + '.' + key;
        const sqlColumnName = getColumNameForJsonField(fullKey);
        if (numberOfEntries > 1) {

            subQuery = subQuery + sqlColumnName + ' = ? and  ';

        } else {
            subQuery = subQuery + sqlColumnName + ' = ? ';
        }
        valArray.push(value);
        numberOfEntries = numberOfEntries - 1;
    }
    return {
        'getQuery': subQuery,
        'valArray': valArray
    };
}

/**
 * It takes a table name and a query object and returns a query string and an array of values
 * @param {string} tableName - The name of the table in which the data is stored.
 * @param{Object} queryObject - The object that you want to search for.
 * @param {Object} options Optional parameter to add pagination.
 * @param {number} options.pageOffset specify which row to start retrieving documents from. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @param {number} options.pageLimit specify number of documents to retrieve. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @private
 */
function _prepareQueryOfIndexSearch(tableName, queryObject, options) {
    let getQuery = `SELECT ${PRIMARY_COLUMN},${JSON_COLUMN} FROM ${tableName} WHERE `;
    const result = _prepareQueryForNestedObject(queryObject);
    return {
        'getQuery': getQuery + result.getQuery + ` ${_getLimitString(options)}`,
        'valArray': result.valArray
    };
}

/**
 * _queryIndex() is a function that takes a queryParams object, a resolve function, and a reject function as parameters.
 * It then executes the query in the queryParams object, and if the query is successful, it returns the results of
 * the query to the resolve function. If the query is unsuccessful, it returns the error to the reject function
 * @param {Object} queryParams - This is an object that contains the query and the values to be used in the query.
 * @param {Function}resolve - a function that takes a single argument, which is the result of the query.
 * @param {Function} reject - a function that will be called if the query fails.
 * @returns {Array} An array of objects
 * @private
 */
function _queryIndex(queryParams, resolve, reject) {
    CONNECTION.execute(queryParams.getQuery, queryParams.valArray,
        function (err, results, _fields) {
            //TODO: emit success or failure metrics based on return value
            if (err) {
                reject(err);
                return;
            }
            if (results && results.length > 0) {
                const retResults = [];
                for (const result of results) {
                    result[JSON_COLUMN].documentId = result[PRIMARY_COLUMN];
                    retResults.push(result[JSON_COLUMN]);
                }
                resolve(retResults);
                return;
            }
            resolve([]);
        });
}

/**
 * It takes a table name, a column name, and a query object, and returns a promise that resolves to an array of objects
 * NB: This query will return only 1000 entries.
 * @example <caption> Sample code </caption>
 * const tableName = 'customer';
 * const queryObject = {
 *             'lastName': 'Alice',
 *             'Age': 100
 *             };
 * try {
 *      const queryResults = await getFromIndex(tableName, queryObject);
 *      console.log(JSON.stringify(queryResults));
 * } catch (e) {
 *      console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table in which the data is stored.
 * @param {Object} queryObject - This is the object that you want to search for.
 * @param {Object} options Optional parameter to add pagination.
 * @param {number} options.pageOffset specify which row to start retrieving documents from. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @param {number} options.pageLimit specify number of documents to retrieve. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @returns {Promise} - A promise; on promise resolution returns array of matched  values in json column. if there are
 * no matches returns empty array. if there are any errors will throw an exception
 */
export function getFromIndex(tableName, queryObject, options = {}) {

    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before findFromIndex');
            return;
        }
        if (!isObject(queryObject) || isObjectEmpty(queryObject)) {
            reject(`please provide valid queryObject`);
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        let queryParseDone = false;
        try {
            const queryParams = _prepareQueryOfIndexSearch(tableName, queryObject, options);
            queryParseDone = true;
            _queryIndex(queryParams, resolve, reject);
        } catch (e) {
            if (!queryParseDone) {
                reject(e.message); // return helpful error messages from query parser
                return;
            }
            const errorMessage = `Exception occurred while querying index`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * Updates the document in the database. Conditional updates are also supported with the optional condition parameter.
 * This api will overwrite current document with new document.
 * @example <caption> Sample code </caption>
 *
 * const docId = 1234;
 * const document = {
 *             'lastName': 'Alice1',
 *             'Age': 140,
 *             'active': true
 *              };
 * try{
 *      await update(tableName, docId, document);
 * } catch(e){
 *     console.error(JSON.stringify(e));
 * }
 *
 * @example <caption> Conditional update Sample code </caption>
 *
 * const docId = 1234;
 * const document = {
 *             'lastName': 'Alice1',
 *             'Age': 140,
 *             'active': true
 *              };
 * try{
 *      // will update only if the existing document exists and the document has field `Age` with Value 100.
 *      await update(tableName, docId, document, "$.Age=100");
 * } catch(e){
 *     console.error(JSON.stringify(e));
 * }
 * @param tableName - The name of the table to update.
 * @param documentId - The primary key of the document to be updated.
 * @param document - The document to be inserted.
 * @param {string} [condition] - Optional coco query condition of the form "$.cost<35" that must be satisfied
 * for update to happen. See query API for more details on how to write coco query strings.
 * @returns {Promise<string>} A promise resolves with documentId if success, or rejects if update failed
 * as either document not found or the condition not satisfied.
 */
export function update(tableName, documentId, document, condition) {
    return new Promise((resolve, reject) => {
        if (!CONNECTION) {
            reject('Please call init before update');
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }

        if (!isObject(document)) {
            reject('Please provide valid document');
            return;
            //Todo: Emit metrics
        }
        try {
            let updateQuery = `UPDATE ${tableName} SET ${JSON_COLUMN} = ? WHERE ${PRIMARY_COLUMN} = ?;`;
            if(condition){
                const sqlCondition = Query.transformCocoToSQLQuery(condition, []);
                updateQuery = `UPDATE ${tableName} SET ${JSON_COLUMN} = ? WHERE ${PRIMARY_COLUMN} = ? AND (${sqlCondition});`;
            }
            CONNECTION.execute(updateQuery, [document, documentId],
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        LOGGER.error({err, tableName, documentId, operation: 'update'}, 'Error updating document');
                        reject(err);
                        return;
                    }
                    if (_results.affectedRows !== 1 && !condition) {
                        reject('Not updated- unable to find documentId');
                        return;
                    }
                    if (_results.affectedRows !== 1 && condition) {
                        reject('Not updated- condition failed or unable to find documentId');
                        return;
                    }
                    resolve(documentId);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while writing to database ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * Modifies the sql query for JSON increment. includes a condition if provided
 * @param {string} tableName - The name of the table in which the document is stored.
 * @param {Object} fieldToIncrementMap - JSON object that contains fields to increment and their values.
 * @param {string} [condition] - Optional coco query condition that must be satisfied for increment to happen.
 * @returns {string} The SQL query string for the increment operation.
 * @private
 */
function _prepareSqlForJsonIncrement(tableName, fieldToIncrementMap, condition) {
    let query = `UPDATE ${tableName} SET document = JSON_SET(document,`;
    let numberOfKeys = Object.keys(fieldToIncrementMap).length;

    for (const key in fieldToIncrementMap) {
        if (numberOfKeys > 1) {
            query += `'$.${key}', IFNULL(JSON_EXTRACT(document, '$.${key}'), 0) + ${fieldToIncrementMap[key]},`;
        } else {
            query += `'$.${key}', IFNULL(JSON_EXTRACT(document, '$.${key}'), 0) + ${fieldToIncrementMap[key]}`;
        }
        --numberOfKeys;
    }
    //mysql> UPDATE customers SET document = json_set(document, '$.age', JSON_EXTRACT(document, '$.age') + 1,'$.id',
    // JSON_EXTRACT(document,'$.id') +1) where documentID = '2';
    query += `) WHERE ${PRIMARY_COLUMN} = ?`;

    // Add condition if provided
    if (condition) {
        const sqlCondition = Query.transformCocoToSQLQuery(condition, []);
        query += ` AND (${sqlCondition})`;
    }

    return query;
}

/**
 * mathAdd increments the value of a field in the document.
 * Conditional increments are also supported with the optional condition parameter.
 * @example <caption> Sample code </caption>
 *
 * const docId = 1234;
 * const increments = {
 *     'visits': 1,
 *     'score': 5
 * };
 * try {
 *     await mathAdd(tableName, docId, increments);
 * } catch(e) {
 *     console.error(JSON.stringify(e));
 * }
 *
 * @example <caption> Conditional increment Sample code </caption>
 *
 * const docId = 1234;
 * const increments = {
 *     'visits': 1,
 *     'score': 5
 * };
 * try {
 *     // will increment only if the existing document has field `active` with value true
 *     await mathAdd(tableName, docId, increments, "$.visits=1");
 * } catch(e) {
 *     console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table in which the document is stored.
 * @param {string} documentId - The primary key of the document you want to update.
 * @param {Object} jsonFieldsIncrements - This is a JSON object that contains the fields to be incremented and the
 * value by which they should be incremented.
 * @param {string} [condition] - Optional coco query condition of the form "$.visits=1" that must be satisfied
 * for increment to happen. See query API for more details on how to write coco query strings.
 * @returns {Promise<boolean>} A promise resolves with true if success, or rejects if increment failed
 * as either document not found or the condition not satisfied.
 */
export function mathAdd(tableName, documentId, jsonFieldsIncrements, condition) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before mathAdd');
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(documentId)) {
            reject('Please provide valid documentID');
            return;
            //Todo: Emit metrics
        }
        if (isObjectEmpty(jsonFieldsIncrements)) {
            reject('please provide valid increments for json filed');
            return;
        }
        for (const key in jsonFieldsIncrements) {
            if (!isNumber(jsonFieldsIncrements[key])) {
                reject('increment can be done only with numerical values');
                return;
            }
        }
        try {
            const incQuery = _prepareSqlForJsonIncrement(tableName, jsonFieldsIncrements, condition);
            CONNECTION.execute(incQuery, [documentId],
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    if (_results.affectedRows === 0 && !condition) {
                        reject('unable to find documentId');
                        return;
                    }
                    if (_results.affectedRows === 0 && condition) {
                        reject('Not updated - condition failed or unable to find documentId');
                        return;
                    }
                    resolve(true);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while incrementing json fields ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * Prepares and SQL query out of the coco query and returns the sql query string.
 * @param {string} tableName - The name of the table in which the data is stored.
 * @param {string} queryString - The cocDB query string.
 * @param {Array[string]} indexedFieldsArray - List of indexed fields in the document.
 * @param {Object} options
 * @param {number} options.pageOffset specify which row to start retrieving documents from. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @param {number} options.pageLimit specify number of documents to retrieve. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @return {string} the sql query as string
 * @private
 */
function _prepareQuery(tableName, queryString, indexedFieldsArray, options) {
    let sqlQuery = Query.transformCocoToSQLQuery(queryString, indexedFieldsArray);
    return `SELECT ${PRIMARY_COLUMN},${JSON_COLUMN} FROM ${tableName}`
        + ` WHERE ${sqlQuery} ${_getLimitString(options)}`;
}

/**
 * Executes the given sql query and if the query is successful, it returns the results of the query to the resolve
 * function or an empty array if no matches were found.
 * If the query is unsuccessful, it returns the error to the reject function.
 * @param {string} sqlQuery - the sql query to execute
 * @param {Function} resolve - a function that takes a single argument, which is the result of the query.
 * @param {Function} reject - a function that will be called if the query fails.
 * @returns {Array} An array of objects
 * @private
 */
function _executeQuery(sqlQuery, resolve, reject) {
    CONNECTION.execute(sqlQuery,
        function (err, results, _fields) {
            //TODO: emit success or failure metrics based on return value
            if (err) {
                reject(err);
                return;
            }
            if (results && results.length > 0) {
                const retResults = [];
                for (const result of results) {
                    result[JSON_COLUMN].documentId = result[PRIMARY_COLUMN];
                    retResults.push(result[JSON_COLUMN]);
                }
                resolve(retResults);
                return;
            }
            resolve([]); // no results found
        });
}

/**
 * Execute a cocoDB query and return the documents matching the query. You can optionally specify a list of indexed
 * fields to search on the index instead of scanning the whole table.
 * @example <caption> A Sample coco query </caption>
 * const tableName = 'customer';
 * const queryString = `NOT($.customerID = 35 && ($.price.tax < 18 OR ROUND($.price.amount) != 69))`;
 * try {
 *      const queryResults = await query(tableName, queryString, ["customerID"]); // customerID is indexed field
 *      console.log(JSON.stringify(queryResults));
 * } catch (e) {
 *      console.error(JSON.stringify(e));
 * }
 *
 * ## cocodb query syntax
 * cocodb query syntax closely resembles mysql query syntax. The following functions are supported as is:
 *
 * ## `$` is a special character that denotes the JSON document itself.
 * All json field names should be prefixed with a `$.` symbol. For Eg. field `x.y` should be given
 * in query as `$.x.y`.
 * It can be used for json compare as. `JSON_CONTAINS($,'{"name": "v"}')`.
 * ** WARNING: JSON_CONTAINS this will not use the index. We may add support in future, but not presently. **
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
 *     "JSON_ARRAY", "JSON_ARRAY_APPEND", "JSON_ARRAY_INSERT", "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_DEPTH",
 *     "JSON_INSERT", "JSON_KEYS", "JSON_LENGTH", "JSON_MERGE_PATCH", "JSON_MERGE_PRESERVE", "JSON_OBJECT",
 *     "JSON_OVERLAPS", "JSON_QUOTE", "JSON_REMOVE", "JSON_REPLACE", "JSON_SEARCH", "JSON_SET", "JSON_TYPE",
 *     "JSON_UNQUOTE", "JSON_VALID", "JSON_VALUE", "MEMBER OF"
 * #### Other Keywords
 *     "LIKE", "NOT", "REGEXP", "RLIKE", "NULL", "AND", "OR", "IS", "BETWEEN", "XOR"
 * @param {string} tableName - The name of the table in which the data is stored.
 * @param {string} queryString The query as string.
 * @param {Array<String>} useIndexForFields A string array of field names for which the index should be used. Note
 * that an index should first be created using `createIndexForJsonField` API. Eg. ['customerID', 'price.tax']
 * @param {Object} options Optional parameter to add pagination.
 * @param {number} options.pageOffset specify which row to start retrieving documents from. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @param {number} options.pageLimit specify number of documents to retrieve. Eg: to get 10 documents from
 * the 100'th document, you should specify `pageOffset = 100` and `pageLimit = 10`
 * @returns {Promise} - A promise; on promise resolution returns array of matched  values in json column. if there are
 * no matches returns empty array. if there are any errors will throw an exception
 */
export function query(tableName, queryString, useIndexForFields = [], options ={}) {

    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before findFromIndex');
            return;
        }
        if (!isString(queryString) || isStringEmpty(queryString)) {
            reject(`please provide valid queryString`);
            return;
        }
        if (!_isValidTableName(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        let queryParseDone = false;
        try {
            const sqlQuery = _prepareQuery(tableName, queryString, useIndexForFields, options);
            queryParseDone = true;
            _executeQuery(sqlQuery, resolve, reject);
        } catch (e) {
            if (!queryParseDone) {
                reject(e.message); // return helpful error messages from query parser
                return;
            }
            reject("Exception occurred while querying");
            //TODO: Emit Metrics
        }
    });
}

// public APIs.
const DB = {
    deleteDataBase,
    createDataBase,
    createTable,
    get,
    put,
    deleteKey,
    deleteDocuments,
    getFromNonIndex,
    deleteTable,
    createIndexForJsonField,
    getFromIndex,
    query,
    update,
    init,
    close,
    DATA_TYPES,
    mathAdd
};

export default DB;
