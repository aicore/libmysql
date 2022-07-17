import {setUpMySQL} from '@aicore/libtestutils';

let config = null;
(async function () {
    config = await setUpMySQL();
    process.env.MY_SQL_SERVER = config.host;
    process.env.MY_SQL_SERVER_PORT = config.port;
    process.env.MY_SQL_SERVER_DB = config.database;
    process.env.MY_SQL_USER = config.user;
    process.env.MY_SQL_PASSWORD = config.password;
    console.log(`${config}`);

}());

export function getMySqlConfigs() {
    if (!config) {
        throw  new Error('config not initialized');
    }
    return config;
}
