import mysql from "mysql2";
import {isString} from "@aicore/libcommonutils";

// @INCLUDE_IN_API_DOCS

let CONNECTION;

//TODO: Add UT
export function init(config) {
   CONNECTION = mysql.createConnection(config);
}
//TODO: Add UT
export function close() {
    CONNECTION.close();
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

function _isValidJsonValue(value) {
    if (!value) {
        return false;
    }
    try {
        JSON.parse(value);
        return true;
    } catch (e) {
        return false;
    }
}

function _handleSqlQueryResponse(resolve, reject, err, results, fields) {
    if (err) {
        reject(err);
        return false;
    }
    resolve({
        results: results, fields: fields
    });
    return true;
}

export function createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn) {
    return new Promise(function (resolve, reject) {
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
                function (err, results, fields) {
                    //TODO: emit success or failure metrics based on return value
                    _handleSqlQueryResponse(resolve, reject, err, results, fields);
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
        if (!_isValidJsonValue(valueForJsonColumn)) {
            reject('Please provide valid JSON String column');
            return;
            //Todo: Emit metrics
        }
        const updateQuery = `INSERT INTO ${tableName} (${nameOfPrimaryKey}, ${nameOfJsonColumn})
                                    values(?,?) ON DUPLICATE KEY UPDATE ${nameOfJsonColumn}=?`;
        try {
            CONNECTION.execute(updateQuery, [primaryKey, valueForJsonColumn, JSON.stringify(valueForJsonColumn)],
                function (err, results, fields) {
                    //TODO: emit success or failure metrics based on return value
                    _handleSqlQueryResponse(resolve, reject, err, results, fields);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while writing to database ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }


    });
}

export function get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn) {
    return new Promise(function (resolve, reject) {

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
                function (err, results, fields) {
                    //TODO: emit success or failure metrics based on return value
                    _handleSqlQueryResponse(resolve, reject, err, results, fields);
                });
        } catch (e) {
            const errorMessage = `Exception occurred while getting data ${e.stack}`;
            reject(errorMessage);
            //TODO: Emit Metrics
        }
    });
}
