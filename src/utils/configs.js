import mysql from 'mysql2';
import {v4 as uuidv4} from 'uuid';

/**
 * This function helps to get configurations for mySQL DB
 *
 */
export function getMySqlConfigs() {
    const host = process.env.MY_SQL_SERVER || 'localhost';
    const port = process.env.MY_SQL_SERVER_PORT || '3306';
    const database = process.env.MY_SQL_SERVER_DB || uuidv4();
    const user = process.env.MY_SQL_USER || uuidv4();
    const password = process.env.MY_SQL_PASSWORD || uuidv4();
    return {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    };
}
