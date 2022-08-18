import mysql from "mysql2";
import {isObject, isObjectEmpty, isString} from "@aicore/libcommonutils";
import crypto from "crypto";

// @INCLUDE_IN_API_DOCS

let CONNECTION = null;

export const PRIMARY_COLUMN = 'documentID';
export const JSON_COLUMN = 'document';


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
 *    "database": "testdb",
 *    "user" : "root",
 *    "password": "1234"
 *  };
 *
 * @example <caption> Sample initialization code  </caption>
 *
 * // set  following  environment variables to access database securely
 * // set MY_SQL_SERVER for mysql server
 * // set MY_SQL_SERVER_PORT to set server port
 * // set MY_SQL_SERVER_DB to specify database where database operations are conducted
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
 * @param {string} config.database - name of database to connect
 * @param {string} config.user - username of database
 * @param {string} config.password - password of database username
 * @returns {boolean}  true if connection is successful false otherwise
 *
 *
 **/
export function init(config) {

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
    if (!isString(config.database)) {
        throw new Error('Please provide valid database');
    }
    if (CONNECTION) {
        console.log(`${CONNECTION}`);
        throw  new Error('One connection is active please close it before reinitializing it');
    }
    try {
        CONNECTION = mysql.createConnection(config);
        return true;
    } catch (e) {
        console.error(e);
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
    CONNECTION.close();
    CONNECTION = null;
}

// https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
const MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME = 64;
const SIZE_OF_PRIMARY_KEY = 128;
const REGX_TABLE_ATTRIBUTES = new RegExp(/^\w+$/);

function _isValidTableAttributes(tableAttributeName) {
    return (isString(tableAttributeName) && tableAttributeName.length <=
        MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME && REGX_TABLE_ATTRIBUTES.test(tableAttributeName));
}

function _isValidPrimaryKey(key) {
    return isString(key) && key.length > 0 && key.length <= SIZE_OF_PRIMARY_KEY;
}

/** This function helps to create a  table in database
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
 * const nameOfPrimaryKey = 'name';
 * const nameOfJsonColumn = 'details';
 * try {
 *   await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
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
            throw new Error('Please call init before createTable');
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
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
                        console.error(`Error occurred while while creating table ${JSON.stringify(err)}`);
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
 * It takes a table name, a primary key, a json column name, and a json value, and inserts the json value into the json
 * column. If the primary key already exists, it updates the json column with the new value
 * @example <caption> Sample code </caption>
 *
 * try {
 *       const primaryKey = 'bob';
 *       const valueOfJson = {
 *           'lastName': 'Alice',
 *           'Age': 100,
 *           'active': true
 *       };
 *       await put('hello', nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
 *   } catch (e) {
 *       console.error(JSON.stringify(e));
 *  }
 *
 * @param {string} tableName - The name of the table in which you want to store the data.
 * @param {Object} document - The JSON string that you want to store in the database.
 * @returns {Promise} A promise on resolving the promise will give documentID throws an exception
 * otherwise
 */
export function put(tableName, document) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before put');
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
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
                        console.error(`Error occurred while while put  ${JSON.stringify(err)}`);
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

function createDocumentId() {
    return crypto.randomBytes(64).toString('hex');
}

/**
 * It deletes a row from the database based on the primary key
 * @example <caption> Sample code </caption>
 *
 * const tableName = 'customer';
 * const nameOfPrimaryKey = 'name';
 * const primaryKey = 'bob';
 * try {
 *    await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
 * } catch(e) {
 *    console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table in which the key is to be deleted.
 * @param {string} documentID -  document id to be deleted
 * @returns {Promise}A promise `resolve` promise to get status of delete. promise will resolve to true
 * for success and  throws an exception for failure.
 */
export function deleteKey(tableName, documentID) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before deleteKey');
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(documentID)) {
            reject('Please provide valid documentID');
            return;
            //Todo: Emit metrics
        }

        const deleteQuery = `DELETE FROM ${tableName} WHERE ${PRIMARY_COLUMN}= ?;`;
        try {
            CONNECTION.execute(deleteQuery, [documentID],
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        console.error(`Error occurred while while delete key  ${JSON.stringify(err)}`);
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
 * It takes in a table name, a primary key name, a primary key value, and a json column name, and returns a promise that
 * resolves to the json column value
 * @example <caption> sample code </caption>
 * const tableName = 'customer';
 * const nameOfPrimaryKey = 'name';
 * const nameOfJsonColumn = 'details';
 * const primaryKey = 'bob';
 * try {
 *     const results = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
 *     console.log(JSON.stringify(result));
 * } catch(e){
 *     console.error(JSON.stringify(e));
 * }
 *
 *
 * @param {string} tableName - The name of the table in which the data is stored.
 * @param {string} documentID - The primary key of the row you want to get.
 * @returns {Promise} A promise on resolve promise to get the value stored for primary key
 */
export function get(tableName, documentID) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before get');
        }
        if (!_isValidTableAttributes(tableName)) {
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
            const getQuery = `SELECT ${JSON_COLUMN} FROM ${tableName} WHERE ${PRIMARY_COLUMN} = ?`;
            CONNECTION.execute(getQuery, [documentID],
                function (err, results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    if (results && results.length > 0) {
                        resolve(results[0][JSON_COLUMN]);
                        return;
                    }
                    resolve({});

                });
        } catch (e) {
            const errorMessage = `Exception occurred while getting data ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

function _prepareQueryForScan(nameOfJsonColumn, tableName, queryObject) {

    let getQuery = `SELECT ${nameOfJsonColumn} FROM ${tableName} WHERE `;
    const valArray = [];
    let numberOfEntries = Object.keys(queryObject).length;
    for (const [key, value] of Object.entries(queryObject)) {
        if (numberOfEntries > 1) {
            getQuery = getQuery + `${nameOfJsonColumn}->"$.${key}" = ? and `;

        } else {
            getQuery = getQuery + `${nameOfJsonColumn}->"$.${key}" = ?`;
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
 * It takes a table name, a column name, and a query object, and returns a promise that resolves to the result of a scan of
 * the table
 * @example <caption> sample code </caption>
 * const tableName = 'customer';
 * const nameOfJsonColumn = 'details';
 * const queryObject = {
 *             'lastName': 'Alice',
 *             'Age': 100
 *         };
 * try {
 *     const scanResults = await getFromNonIndex(tableName, nameOfJsonColumn, queryObject);
 *     console.log(JSON.stringify(scanResults));
 * } catch (e){
 *     console.error(JSON.stringify(e));
 * }
 *
 * @param {string} tableName - The name of the table you want to query.
 * @param {Object} queryObject - This is the object that you want to query.
 * @returns {Promise} - A promise; on promise resolution returns array of  matched object from json column. if there are
 * no match returns empty array
 */
export function getFromNonIndex(tableName, queryObject) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before getFromNonIndex');
        }
        if (!isObject(queryObject) || isObjectEmpty(queryObject)) {
            reject(`please provide valid queryObject`);
            return;
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        try {
            const queryParams = _prepareQueryForScan(JSON_COLUMN, tableName, queryObject);
            _queryIndex(queryParams, JSON_COLUMN, resolve, reject);
        } catch (e) {
            const errorMessage = `Exception occurred while getting data ${e.stack}`;
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
            throw new Error('Please call init before getFromNonIndex');
        }

        if (!_isValidTableAttributes(tableName)) {
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

function _buildCreateJsonColumQuery(tableName, nameOfJsonColumn, jsonField, dataTypeOfNewColumn) {

    return `ALTER TABLE ${tableName} ADD COLUMN ${jsonField} ${dataTypeOfNewColumn}  GENERATED ALWAYS` +
        ` AS (${nameOfJsonColumn}->>"$.${jsonField}");`;
}

function _buildCreateIndexQuery(tableName, _nameOfJsonColumn, jsonField, isUnique) {
    if (isUnique) {
        return `CREATE UNIQUE INDEX  idx_${jsonField} ON ${tableName}(${jsonField});`;
    }
    return `CREATE INDEX  idx_${jsonField} ON ${tableName}(${jsonField});`;
}

/*
    private function exporting this for testing
 */
export function _createIndex(resolve, reject, tableName, nameOfJsonColumn, jsonField, isUnique) {

    try {
        const indexQuery = _buildCreateIndexQuery(tableName, nameOfJsonColumn, jsonField, isUnique);
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
 * It creates a new column in the table for the JSON field and then creates an index on that column
 * @example <caption> Sample code </caption>
 * const tableName = 'customer';
 * const nameOfJsonColumn = 'customerDetails';
 * let jsonfield = 'lastName';
 * // supported data types can be found on https://dev.mysql.com/doc/refman/8.0/en/data-types.html
 * let dataTypeOfNewColumn = 'VARCHAR(50)';
 * let isUnique = false;
 * try{
 *      await createIndexForJsonField(tableName, nameOfJsonColumn, jsonfield, dataTypeOfNewColumn, isUnique);
 *      jsonfield = 'Age';
 *      dataTypeOfNewColumn = 'INT';
 *      isUnique = false;
 *
 *      await createIndexForJsonField(tableName, nameOfJsonColumn, jsonfield, dataTypeOfNewColumn, isUnique);
 * } catch (e){
 *      console.error(JSON.stringify(e));
 * }
 * @param {string} tableName - The name of the table in which you want to create the index.
 * @param {string} jsonField - The name of the field in the JSON object that you want to index.
 * @param {string} dataTypeOfNewColumn - This is the data type of the new column that will be created.
 * visit https://dev.mysql.com/doc/refman/8.0/en/data-types.html to know all supported data types
 * @param {boolean} isUnique - If true, the json filed has to be unique for creating index.
 * @returns {Promise} A promise
 */
export function createIndexForJsonField(tableName, jsonField, dataTypeOfNewColumn, isUnique) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before createIndexForJsonField');
        }

        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(jsonField)) {
            reject('please provide valid name for json field');
            return;
            //Todo: Emit metrics
        }
        if (!isString(dataTypeOfNewColumn)) {
            reject('please provide valid  data type for json field');
            return;
        }

        try {
            const createColumnQuery = _buildCreateJsonColumQuery(tableName,
                JSON_COLUMN,
                jsonField,
                dataTypeOfNewColumn);
            CONNECTION.execute(createColumnQuery,
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    return _createIndex(resolve, reject, tableName, JSON_COLUMN, jsonField, isUnique);
                });
        } catch (e) {
            console.error(JSON.stringify(e));
            const errorMessage = `Exception occurred while creating column for JSON field`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

function _prepareQueryOfIndexSearch(tableName, nameOfJsonColumn, queryObject) {
    let getQuery = `SELECT ${nameOfJsonColumn} FROM ${tableName} WHERE `;
    const valArray = [];
    let numberOfEntries = Object.keys(queryObject).length;
    for (const [key, value] of Object.entries(queryObject)) {
        if (numberOfEntries > 1) {
            getQuery = getQuery + `${key} = ? and `;

        } else {
            getQuery = getQuery + `${key} = ?`;
        }
        valArray.push(value);
        numberOfEntries = numberOfEntries - 1;

    }
    return {
        'getQuery': getQuery,
        'valArray': valArray
    };
}

function _queryIndex(queryParams, nameOfJsonColumn, resolve, reject) {
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
                    retResults.push(result[nameOfJsonColumn]);
                }
                resolve(retResults);
                return;
            }
            resolve([]);
        });

}

/**
 * It takes a table name, a column name, and a query object, and returns a promise that resolves to an array of objects
 * @example <caption> Sample code </caption>
 * const tableName = 'customer';
 * const nameOfJsonColumn = 'customerDetails';
 * const queryObject = {
 *             'lastName': 'Alice',
 *             'Age': 100
 *             };
 * try {
 *      const queryResults = await getFromIndex(tableName, nameOfJsonColumn, queryObject);
 *      console.log(JSON.stringify(queryResults));
 * } catch (e) {
 *      console.error(JSON.stringify(e));
 * }
 *
 *
 * @param {string} tableName - The name of the table in which the data is stored.
 * @param {Object} queryObject - This is the object that you want to search for.
 * @returns {Promise} - A promise; on promise resolution returns array of matched  values in json column. if there are
 * no matches returns empty array. if there are any errors will throw an exception
 */
export function getFromIndex(tableName, queryObject) {

    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before findFromIndex');
        }
        if (!isObject(queryObject) || isObjectEmpty(queryObject)) {
            reject(`please provide valid queryObject`);
            return;
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        try {
            const queryParams = _prepareQueryOfIndexSearch(tableName, JSON_COLUMN, queryObject);
            _queryIndex(queryParams, JSON_COLUMN, resolve, reject);
        } catch (e) {
            const errorMessage = `Exception occurred while querying index`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}
