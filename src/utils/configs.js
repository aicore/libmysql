import crypto from 'crypto';
/**
 * This function helps to get configurations for mySQL DB
 *
 */
export function getMySqlConfigs() {
    const host = process.env.MY_SQL_SERVER || 'localhost';
    const port = process.env.MY_SQL_SERVER_PORT || '3306';
    const database = process.env.MY_SQL_SERVER_DB || crypto.randomBytes(8).toString('hex');
    const user = process.env.MY_SQL_USER || crypto.randomBytes(8).toString('hex');
    const password = process.env.MY_SQL_PASSWORD || crypto.randomBytes(8).toString('hex');
    return {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    };
}
