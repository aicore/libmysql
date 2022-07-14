/**
 * This function helps to get configurations for mySQL DB
 *
 */
export function getMySqlConfigs() {
    const host = process.env.MY_SQL_SERVER || 'localhost';
    const port = process.env.MY_SQL_SERVER_PORT || '3306';
    const database = process.env.MY_SQL_SERVER_DB || generateRandomString(8);
    const user = process.env.MY_SQL_USER || generateRandomString(8);
    const password = process.env.MY_SQL_PASSWORD || generateRandomString(8);
    return {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    };
}

// Create a random String for database configurations
function generateRandomString(myLength) {
    const chars =
        "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz";
    const randomArray = Array.from(
        {length: myLength},
        (v, k) => chars[Math.floor(Math.random() * chars.length)]
    );
}
