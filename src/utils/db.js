import mysql from "mysql2";
import {isObject, isString} from "@aicore/libcommonutils";

// @INCLUDE_IN_API_DOCS

let CONNECTION = null;

//TODO: Add UT
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
    CONNECTION = mysql.createConnection(config);
}

//TODO: Add UT
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
                        console.error(`Error occurred while while put  ${JSON.stringify(err)}`);
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

export function getFromNonIndex(tableName, nameOfJsonColumn, queryObject) {
    return new Promise(function (resolve, reject) {
        if (!CONNECTION) {
            throw new Error('Please call init before getFromNonIndex');
        }
        if (!isObject(queryObject)) {
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
        } catch (e) {
            const errorMessage = `Exception occurred while getting data ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}
