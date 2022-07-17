import {setUpMySQL} from '@aicore/libtestutils';

let config = null;

async function _init() {
    config = await setUpMySQL();
    process.env.MY_SQL_SERVER = config.host;
    process.env.MY_SQL_SERVER_PORT = config.port;
    process.env.MY_SQL_SERVER_DB = config.database;
    process.env.MY_SQL_USER = config.user;
    process.env.MY_SQL_PASSWORD = config.password;
    console.log(`${config}`);

}

export async function getMySqlConfigs() {
    if (!config) {
        await _init();
    }
    return config;
}
