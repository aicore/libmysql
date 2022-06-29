import {getMySqlConfigs} from "./configs.js";
import mysql from "mysql2";
import {isString} from "@aicore/libcommonutils";

const CONNECTION = mysql.createConnection(getMySqlConfigs());
// https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
const MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME = 64;
const SIZE_OF_PRIMARY_KEY = 255;

function _isValidTableAttributes(tableAttributeName) {
    return (isString(tableAttributeName) && tableAttributeName.length <
        MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME);
}

function _isValidPrimaryKey(key) {
    return isString(key) && key.length <= SIZE_OF_PRIMARY_KEY;
}

function _isValidJsonValue(value) {
    return !(!value);
}

export function createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn) {
    return new Promise(function (resolve, reject) {
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfPrimaryKey)) {
            reject('please provide valid name for primary key');
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            //Todo: Emit metrics
        }
        try {
            const createTableSql = `CREATE TABLE ${tableName} 
                                        (${nameOfPrimaryKey} VARCHAR(${SIZE_OF_PRIMARY_KEY}) NOT NULL PRIMARY KEY, 
                                        ${nameOfJsonColumn} JSON NOT NULL)`;
            CONNECTION.execute(createTableSql,
                function (err, results, fields) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve({
                        results: results,
                        fields: fields
                    });
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
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfPrimaryKey)) {
            reject('please provide valid name for primary key');
            //Todo: Emit metrics
        }
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
            //Todo: Emit metrics
        }
        if (!_isValidPrimaryKey(primaryKey)) {
            reject('Please provide valid primary key');
            //Todo: Emit metrics
        }
        if (!_isValidJsonValue(valueForJsonColumn)) {
            reject('Please provide valid JSON');
            //Todo: Emit metrics
        }

        try {
            const updateQuery = `INSERT INTO ${tableName} (${nameOfPrimaryKey}, ${nameOfJsonColumn})
                                    values(?,?) ON DUPLICATE KEY UPDATE ${nameOfJsonColumn}=?`;
            try {
                CONNECTION.execute(updateQuery, [primaryKey, valueForJsonColumn, valueForJsonColumn],
                    function (err, results, fields) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve({
                            results: results,
                            fields: fields
                        });
                    });
            } catch (e) {
                const errorMessage = `Execution occurred while updating data ${e.stack}`;
                console.error(errorMessage);
                resolve(errorMessage);
                //TODO: Emit Metrics
            }

        } catch (e) {
            const errorMessage = `execution occurred while putting values to table ${e.stack}`;
            console.error(errorMessage);
            reject(errorMessage);
            //Todo: Emit metrics
        }

    });
}

/*
export function getValueForKey(key, column, table) {
    return new Promise((resolve, reject) => {
        const query = `SELECT ${column} FROM ${table} WHERE ${key.column} = '${key.value}'`;
        console.log(query);
        client.query(query, (err, results, fields) => {
            if (err) {
                reject(err);
            }
            resolve(results && results.length > 0 ? results[0] : null);
        });
    });
}

export function createTable(table, keyColumn, valueColumn) {
    return new Promise((resolve, reject) => {
        client.query(`CREATE TABLE ${table} (${keyColumn} VARCHAR(255) NOT NULL, ${valueColumn} JSON NOT NULL, PRIMARY KEY (${keyColumn}))`, (err, results, fields) => {
            if (err) {
                reject(err);
            }
            resolve(results);
        });
    });
}

export function putValueForKey(key, value, table) {
    return new Promise((resolve, reject) => {
        client.query
        client.query(`INSERT INTO ${table} (${key.column}, ${value.column}) VALUES ('${key.value}', '${JSON.stringify(value.value)}') ON DUPLICATE KEY UPDATE ${value.column} =  '${JSON.stringify(value.value)}'`, [key, value], (err, results, fields) => {
            if (err) {
                reject(err);
            }
            resolve(results);
        });
    });
}
*/
