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

export function createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn) {
    return new Promise((resolve, reject) => {
        if (!_isValidTableAttributes(tableName)) {
            reject('please provide valid table name');
        }
        if (!_isValidTableAttributes(nameOfPrimaryKey)) {
            reject('please provide valid name for primary key');
        }
        if (!_isValidTableAttributes(nameOfJsonColumn)) {
            reject('please provide valid name for json column');
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
            const errorMessage = `execution occurred while creating table ${e}`;
            console.error(errorMessage);
            reject(e);
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
