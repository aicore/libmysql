import mysql from "mysql2";
import {isObject, isObjectEmpty, isString} from "@aicore/libcommonutils";
import crypto from "crypto";

// @INCLUDE_IN_API_DOCS

let CONNECTION = null;

/* Defining a constant variable called PRIMARY_COLUMN and assigning it the value of 'documentID'. */
export const PRIMARY_COLUMN = 'documentID';
/* Defining a constant variable called JSON_COLUMN and assigning it the value of 'document'. */
export const JSON_COLUMN = 'document';
/* Creating a constant called DATA_TYPES. It is an object with three properties. The first property is DOUBLE, which is a
string. The second property is VARCHAR, which is a function that takes a parameter. The third property is INT, which is
a string. */
export const DATA_TYPES = {
    // https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
    DOUBLE: 'DOUBLE',
    VARCHAR: function (a = 255) {
        return `VARCHAR(${a})`;
    },
    INT: 'INT'
};


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
/* Defining a constant variable named MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME and assigning it the value 64. */
const MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME = 64;
/* Defining a constant variable called SIZE_OF_PRIMARY_KEY and assigning it the value of 32. */
const SIZE_OF_PRIMARY_KEY = 32;
/* Creating a regular expression that will match any word character (letter, number, or underscore) one or more times. */
const REGX_TABLE_ATTRIBUTES = new RegExp(/^\w+$/);

/**
 * It checks if the table attribute name is a string, and if it is, it checks if the length of the string is less than or
 * equal to the maximum length of a MySQL table name or column name, and if it is, it checks if the string matches the
 * regular expression for a table attribute name
 * @param {string} tableAttributeName - The name of the table attribute.
 * @returns A boolean value.
 */
function _isValidTableAttributes(tableAttributeName) {
    return (isString(tableAttributeName) && tableAttributeName.length <=
        MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME && REGX_TABLE_ATTRIBUTES.test(tableAttributeName));
}

/**
 * Returns true if the given key is a string of length greater than zero and less than or equal to the maximum size of a
 * primary key.
 * @param {string} key - The primary key of the item to be retrieved.
 * @returns A boolean value.
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


/**
 * It generates a random string of 16 hexadecimal characters
 * When converting hexadecimal to string. The generated string will contain 32 characters
 * @returns A random string of hexadecimal characters.
 */
function createDocumentId() {
    return crypto.randomBytes(16).toString('hex');
}

/**
 * It deletes a document from the database based on the document id
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
 * @param {string} tableName - The name of the table in which the key is to be deleted.
 * @param {string} documentID -  document id to be deleted
 * @returns {Promise}A promise `resolve` promise to get status of delete. promise will resolve to true
 * for success and  throws an exception for failure.
 */
export function deleteKey(tableName, documentID) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before deleteKey');
            return;
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

/**
 * It takes a JSON object and returns a SQL query and an array of values to be used in a prepared statement
 * @param {Object} subQueryObject - This is the object that you want to query.
 * @param {string} [parentKey] - This is the parent key of the current object.
 * @returns {Object} An object with two properties: getQuery and valArray.
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
                subResults.valArray.forEach(value => {
                    valArray.push(value);
                });
                numberOfEntries = numberOfEntries - 1;
                continue;
            }
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
 * @returns {Object} An object with two properties: getQuery and valArray.
 */
function _prepareQueryForScan(tableName, queryObject) {
    let getQuery = `SELECT ${JSON_COLUMN} FROM ${tableName} WHERE `;
    const subQuery = _queryScanBuilder(queryObject);
    return {
        'getQuery': getQuery + subQuery.getQuery,
        'valArray': subQuery.valArray
    };
}

/**
 * It takes a table name and a query object, and returns a promise that resolves to the
 * array of matched documents.
 * `NB: this api will not detect boolean fields while scanning`
 * This query is doing database scan. using this query frequently can degrade database performance. if this query
 * is more frequent consider creating index and use `getFromIndex` API
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
 * @returns {Promise} - A promise; on promise resolution returns array of  matched documents. if there are
 * no match returns empty array
 */
export function getFromNonIndex(tableName, queryObject) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before getFromNonIndex');
            return;
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
            const queryParams = _prepareQueryForScan(tableName, queryObject);
            _queryIndex(queryParams, resolve, reject);
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
            reject('Please call init before getFromNonIndex');
            return;
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


/**
 * It takes a table name, a name for the new column, the name of the field in the JSON, and the data type of the new
 * column, and returns a query that will create a new column in the table that is a copy of the field in the JSON column.
 * @param {string} tableName - The name of the table you want to add the column to.
 * @param {string} nameOfJsonColumn - The name of the new column that will be created.
 * @param {string} jsonField - The field in the JSON object that you want to extract.
 * @param {string} dataTypeOfNewColumn - This is the data type of the new column.
 * @returns  {string} A string that is a query to create a new column in a table.
 */
function _buildCreateJsonColumQuery(tableName, nameOfJsonColumn, jsonField, dataTypeOfNewColumn) {
    return `ALTER TABLE ${tableName} ADD COLUMN ${nameOfJsonColumn} ${dataTypeOfNewColumn}  GENERATED ALWAYS` +
        ` AS (${JSON_COLUMN}->>"$.${jsonField}");`;
}

/**
 * It takes a table name, a json field name, and a boolean value indicating whether the index should be unique or not,
 * and returns a string containing the SQL query to create the index
 * @param {string} tableName - The name of the table to create the index on.
 * @param {string} jsonField - The name of the JSON field that you want to index.
 * @param {boolean} isUnique - If true, the index will be unique.
 * @returns {string} A string that is a query to create an index on a table.
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

/* Creating a regular expression that will match a string that is a valid JSON field. */
const REGX_JSON_FIELD = new RegExp(/^\w+(\.?\w)*$/);

/**
 * It checks if the jsonField is a valid json field.
 * @param {string}jsonField - The JSON field to be queried.
 * @returns {boolean} if its valid json field false otherwise
 */
function _isJsonField(jsonField) {
    return isString(jsonField) && REGX_JSON_FIELD.test(jsonField);
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
 * @param {string} jsonField - The name of the field in the JSON object that you want to index.
 * @param {string} dataTypeOfNewColumn - This is the data type of the new column that will be created.
 * visit https://dev.mysql.com/doc/refman/8.0/en/data-types.html to know all supported data types
 * @param {boolean} isUnique - If true, the json filed has to be unique for creating index.
 * @returns {Promise} A promise
 */
export function createIndexForJsonField(tableName, jsonField, dataTypeOfNewColumn, isUnique) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before createIndexForJsonField');
            return;
        }

        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            return;
            //Todo: Emit metrics
        }
        if (!_isJsonField(jsonField)) {
            reject('please provide valid name for json field');
            return;
            //Todo: Emit metrics
        }
        if (!isString(dataTypeOfNewColumn)) {
            reject('please provide valid  data type for json field');
            return;
        }
        const sqlJsonColumn = jsonField.replaceAll('.', '');

        try {
            const createColumnQuery = _buildCreateJsonColumQuery(tableName,
                sqlJsonColumn,
                jsonField,
                dataTypeOfNewColumn);
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
            console.error(JSON.stringify(e));
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
 */
function _prepareQueryForNestedObject(subQueryObject, parentKey = "") {
    const valArray = [];
    let subQuery = '';
    let numberOfEntries = Object.keys(subQueryObject).length;
    for (const [key, value] of Object.entries(subQueryObject)) {
        if (isObject(value)) {
            let subResults = _prepareQueryForNestedObject(value, key);
            if (subResults) {
                subQuery += parentKey + subResults.getQuery;
                subResults.valArray.forEach(value => {
                    valArray.push(value);
                });
            }
            numberOfEntries = numberOfEntries - 1;
            continue;
        }
        if (numberOfEntries > 1) {
            subQuery = subQuery + `${parentKey}${key} = ? and  `;

        } else {
            subQuery = subQuery + `${parentKey}${key} = ? `;
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
 */
function _prepareQueryOfIndexSearch(tableName, queryObject) {
    let getQuery = `SELECT ${JSON_COLUMN} FROM ${tableName} WHERE `;
    const result = _prepareQueryForNestedObject(queryObject);
    return {
        'getQuery': getQuery + result.getQuery,
        'valArray': result.valArray
    };
}

/**
 * _queryIndex() is a function that takes a queryParams object, a resolve function, and a reject function as parameters. It
 * then executes the query in the queryParams object, and if the query is successful, it returns the results of the query
 * to the resolve function. If the query is unsuccessful, it returns the error to the reject function
 * @param {Object} queryParams - This is an object that contains the query and the values to be used in the query.
 * @param {Function}resolve - a function that takes a single argument, which is the result of the query.
 * @param {Function} reject - a function that will be called if the query fails.
 * @returns {Array} An array of objects
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
 * @returns {Promise} - A promise; on promise resolution returns array of matched  values in json column. if there are
 * no matches returns empty array. if there are any errors will throw an exception
 */
export function getFromIndex(tableName, queryObject) {

    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            reject('Please call init before findFromIndex');
            return;
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
            const queryParams = _prepareQueryOfIndexSearch(tableName, queryObject);
            _queryIndex(queryParams, resolve, reject);
        } catch (e) {
            const errorMessage = `Exception occurred while querying index`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}

/**
 * It updates the document in the database
 * This api will overwrite current document with new document
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
 * @param tableName - The name of the table to update.
 * @param documentId - The primary key of the document to be updated.
 * @param document - The document to be inserted.
 * @returns {Promise} A promise on resolving promise will get documentId
 */
export function update(tableName, documentId, document) {
    return new Promise((resolve, reject) => {
        if (!CONNECTION) {
            reject('Please call init before update');
            return;
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
        const updateQuery = `UPDATE ${tableName} SET ${JSON_COLUMN} = ? WHERE ${PRIMARY_COLUMN} = ?;`;
        try {
            CONNECTION.execute(updateQuery, [document, documentId],
                function (err, _results, _fields) {
                    //TODO: emit success or failure metrics based on return value
                    if (err) {
                        console.error(`Error occurred while while updating data  ${JSON.stringify(err)}`);
                        reject(err);
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
