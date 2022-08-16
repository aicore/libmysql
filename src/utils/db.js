import mysql from "mysql2";
import {isObject, isObjectEmpty, isString} from "@aicore/libcommonutils";

// @INCLUDE_IN_API_DOCS

let CONNECTION = null;

// @INCLUDE_IN_API_DOCS


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
const SIZE_OF_PRIMARY_KEY = 255;
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
 * @param {string} nameOfPrimaryKey name of primary key
 * @param {string} nameOfJsonColumn name of JsonColumn
 * @return {Promise}  returns a `Promise` await on `Promise` to get status of `createTable`
 * `on success` await will return `true`. `on failure` await will throw an `exception`.
 *
 */
export function createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before createTable');
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfPrimaryKey)) {
            reject('please provide valid name for primary key');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            return;
            //Todo: Emit metrics
        }
        try {
            const createTableSql = `CREATE TABLE ${tableName} 
                                        (${nameOfPrimaryKey} VARCHAR(${SIZE_OF_PRIMARY_KEY}) NOT NULL PRIMARY KEY, 
                                        ${nameOfJsonColumn} JSON NOT NULL)`;
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
 * @param {string} nameOfPrimaryKey - The name of the primary key column in the table.
 * @param {string} primaryKey - The primary key of the table.
 * @param {string} nameOfJsonColumn - The name of the column in which you want to store the JSON string.
 * @param {string} valueForJsonColumn - The JSON string that you want to store in the database.
 * @returns {Promise} A promise on resolving the promise will return true it put is successful throws an exception
 * otherwise
 */
export function put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before put');
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfPrimaryKey)) {
            reject('please provide valid name for primary key');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(primaryKey)) {
            reject('Please provide valid primary key');
            return;
            //Todo: Emit metrics
        }

        if (!isObject(valueForJsonColumn)) {
            reject('Please provide valid JSON String column');
            return;
            //Todo: Emit metrics
        }

        const updateQuery = `INSERT INTO ${tableName} (${nameOfPrimaryKey}, ${nameOfJsonColumn})
                                    values(?,?) ON DUPLICATE KEY UPDATE ${nameOfJsonColumn}=?`;
        try {
            CONNECTION.execute(updateQuery, [primaryKey, valueForJsonColumn, valueForJsonColumn],
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        console.error(`Error occurred while while put  ${JSON.stringify(err)}`);
                        reject(err);
                        return;
                    }
                    resolve(true);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while writing to database ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
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
 * @param {string} nameOfPrimaryKey - The name of the primary key in the table.
 * @param {string} primaryKey - The primary key of the row you want to delete.
 * @returns A promise `resolve` promise to get status of delete. promise will resolve to true
 * for success and  throws an exception for failure.
 */
export function deleteKey(tableName, nameOfPrimaryKey, primaryKey) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before deleteKey');
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfPrimaryKey)) {
            reject('please provide valid name for primary key');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(primaryKey)) {
            reject('Please provide valid primary key');
            return;
            //Todo: Emit metrics
        }

        const deleteQuery = `DELETE FROM ${tableName} WHERE ${nameOfPrimaryKey}= ?;`;
        try {
            CONNECTION.execute(deleteQuery, [primaryKey],
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
            const errorMessage = `Exception occurred while deleting key ${primaryKey}  from database ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * It takes in a table name, a primary key name, a primary key value, and a json column name, and returns a promise that
 * resolves to the json column value
 * @param tableName - The name of the table in which the data is stored.
 * @param nameOfPrimaryKey - The name of the primary key column in the table.
 * @param primaryKey - The primary key of the row you want to get.
 * @param nameOfJsonColumn - The name of the column in the table that contains the JSON data.
 * @returns A promise
 */
export function get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before get');
        }
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfPrimaryKey)) {
            reject('please provide valid name for primary key');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(primaryKey)) {
            reject('Please provide valid primary key');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            return;
            //Todo: Emit metrics
        }
        try {
            const getQuery = `SELECT ${nameOfJsonColumn} FROM ${tableName} WHERE ${nameOfPrimaryKey} = ?`;
            CONNECTION.execute(getQuery, [primaryKey],
                function (err, results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    if (results && results.length > 0) {
                        resolve(results[0][nameOfJsonColumn]);
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
 * @param tableName - The name of the table you want to query.
 * @param nameOfJsonColumn - The name of the column that contains the JSON data.
 * @param queryObject - This is the object that you want to query.
 * @returns A promise
 */
export function getFromNonIndex(tableName, nameOfJsonColumn, queryObject) {
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
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            return;
            //Todo: Emit metrics
        }
        try {
            const queryParams = _prepareQueryForScan(nameOfJsonColumn, tableName, queryObject);
            _queryIndex(queryParams, nameOfJsonColumn, resolve, reject);
        } catch (e) {
            const errorMessage = `Exception occurred while getting data ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * It deletes a table from the database
 * @param tableName - The name of the table to be deleted.
 * @returns A promise that will resolve to true if the table is deleted, or reject with an error if the table is not
 * deleted.
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
        console.log(indexQuery);
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
 * @param tableName - The name of the table in which you want to create the index.
 * @param nameOfJsonColumn - The name of the JSON column in the table.
 * @param jsonField - The name of the field in the JSON object that you want to index.
 * @param dataTypeOfNewColumn - This is the data type of the new column that will be created.
 * @param isUnique - If true, the index will be unique.
 * @returns A promise
 */
export function createIndexForJsonField(tableName, nameOfJsonColumn, jsonField, dataTypeOfNewColumn, isUnique) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before createIndexForJsonField');
        }

        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            return;
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(jsonField)) {
            reject('please provide valid name for json field');
            return;
            //Todo: Emit metrics
        }
        if (!isString(dataTypeOfNewColumn)) {
            reject('please provide valid  data type for json column');
            return;
        }

        try {
            const createColumnQuery = _buildCreateJsonColumQuery(tableName,
                nameOfJsonColumn,
                jsonField,
                dataTypeOfNewColumn);
            CONNECTION.execute(createColumnQuery,
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        reject(err);
                        return;
                    }
                    return _createIndex(resolve, reject, tableName, nameOfJsonColumn, jsonField, isUnique);
                });
        } catch (e) {
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
 * @param tableName - The name of the table in which the data is stored.
 * @param nameOfJsonColumn - The name of the column in the table that contains the JSON data.
 * @param queryObject - This is the object that you want to search for.
 * @returns A promise
 */
export function getFromIndex(tableName, nameOfJsonColumn, queryObject) {

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
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            return;
            //Todo: Emit metrics
        }
        try {
            const queryParams = _prepareQueryOfIndexSearch(tableName, nameOfJsonColumn, queryObject);
            _queryIndex(queryParams, nameOfJsonColumn, resolve, reject);
        } catch (e) {
            const errorMessage = `Exception occurred while querying index`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}
