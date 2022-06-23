import {getMySqlConfigs} from "./configs.js";
import mysql from "mysql2";

const client = mysql.createConnection(getMySqlConfigs());

export function getValueForKey(key, table) {
    return new Promise((resolve, reject) => {
        client.query(`SELECT * FROM ${table} WHERE key = ?`, [key], (err, results, fields) => {
            if (err) {
                reject(err);
            }
            resolve(results[0].value);
        });
    });
}

export function createTable(table, key, value) {
    return new Promise((resolve, reject) => {
        client.query(`CREATE TABLE ${table} (${key} VARCHAR(255) NOT NULL, ${value} JSON NOT NULL, PRIMARY KEY (${key}))`, (err, results, fields) => {
            if (err) {
                reject(err);
            }
            resolve(results);
        });
    });
}
