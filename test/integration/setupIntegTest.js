import {setUpMySQL} from '@aicore/libtestutils';

let config = null;
/* uncomment these to test locally in webstorm
config = {
    'host': 'localhost',
    'port': '3306',
    'user': 'root',
    'password': '1234'

};
*/
async function _init() {
    if (!config) {
        config = await setUpMySQL();
    }

    process.env.MY_SQL_SERVER = config.host;
    process.env.MY_SQL_SERVER_PORT = config.port;
    process.env.MY_SQL_SERVER_DB = config.database;
    process.env.MY_SQL_USER = config.user;
    process.env.MY_SQL_PASSWORD = config.password;
    console.log(`${JSON.stringify(config)}`);

}

export async function getMySqlConfigs() {
    await _init();
    return config;
}
