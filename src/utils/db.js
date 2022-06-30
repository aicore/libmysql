import {getMySqlConfigs} from "./configs.js";
import mysql from "mysql2";
import {isString} from "@aicore/libcommonutils";

const CONNECTION = mysql.createConnection(getMySqlConfigs());

// https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
const MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME = 64;
const SIZE_OF_PRIMARY_KEY = 255;
const REGX_TABLE_ATTRIBUTES = new RegExp(/^\w*$/);

function _isValidTableAttributes(tableAttributeName) {
    return (isString(tableAttributeName) && tableAttributeName.length <
        MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME && REGX_TABLE_ATTRIBUTES.test(tableAttributeName));
}

function _isValidPrimaryKey(key) {
    return isString(key) && key.length <= SIZE_OF_PRIMARY_KEY;
}

function _isValidJsonValue(value) {
    return !(!value);
}

function _handleSqlQueryResponse(resolve, reject, err, results, fields) {
    if (err) {
        reject(err);
        return false;
    }
    resolve({
        results: results,
        fields: fields
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
            console.error(errorMessage);
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
            reject('Please provide valid JSON');
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
            const errorMessage = `Execution occurred while updating data ${e.stack}`;
            console.error(errorMessage);
            resolve(errorMessage);
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
            CONNECTION.execute(getQuery, [primaryKey], function (err, results, fields) {
                //TODO: emit success or failure metrics based on return value
                _handleSqlQueryResponse(resolve, reject, err, results, fields);
            });
        } catch (e) {
            const errorMessage = `Execution occurred while getting data ${e.stack}`;
            console.error(errorMessage);
            resolve(errorMessage);
            //TODO: Emit Metrics
        }
    });
}
