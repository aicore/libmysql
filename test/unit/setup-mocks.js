import mysql from "mysql2";

let setupDone = false;

let mockedFunctions = {
    connection: {
        execute: function (sql, callback) {
            callback(null, {
                ResultSetHeader: {
                    fieldCount: 0,
                    affectedRows: 0,
                    insertId: 0,
                    info: '',
                    serverStatus: 2,
                    warningStatus: 0
                }
            }, undefined);
        },
        close : function (){}
    }
};

function _setup() {
    if (setupDone) {
        return;
    }
    mysql.createConnection = function () {
        return mockedFunctions.connection;
    };
    setupDone = true;
}

_setup();

export default mockedFunctions;
