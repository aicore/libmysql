/*global describe, it, beforeEach*/
import mockedFunctions from '../setup-mocks.js';
import * as chai from 'chai';
import {createTable, get, put, init, close, deleteKey, getFromNonIndex, deleteTable} from "../../../src/utils/db.js";
import {getMySqlConfigs} from "@aicore/libcommonutils";

let expect = chai.expect;

function generateStringSequence(seed, length) {
    let genString = [];
    let char = seed;
    let charCode = seed.charCodeAt(0);

    for (let i = 0; i < length; i++) {
        genString.push(char);
        charCode = charCode + 1;
        char = String.fromCharCode(charCode);
    }
    return genString.join('');
}

function generateAValidString(len) {
    let genString = [];
    for (let i = 0; i < len; i++) {
        genString.push('a');
    }
    return genString.join('');
}

describe('Unit tests for db.js', function () {
    beforeEach(function () {
        close();
        init(getMySqlConfigs());
    });

    it('create table api should fail  if connection not initialised', async function () {
        try {
            close();
            const tableName = '';
            const nameOfPrimaryKey = 'id';
            const nameOfJsonColumn = 'column';
            await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);

        } catch (e) {
            expect(e.toString()).to.eql('Error: Please call init before createTable');
        }
    });

    it('create table api should fail for invalid table name', async function () {
        try {
            const tableName = '';
            const nameOfPrimaryKey = 'id';
            const nameOfJsonColumn = 'column';
            await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);

        } catch (e) {
            expect(e).to.eql('please provide valid table name');
        }
    });
    it('create table api should fail for invalid table name', async function () {
        try {
            const tableName = null;
            const nameOfPrimaryKey = 'id';
            const nameOfJsonColumn = 'column';
            await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);

        } catch (e) {
            expect(e).to.eql('please provide valid table name');
        }
    });
    it('create table api should fail  for invalid primary key name', async function () {
        try {
            const tableName = 'hello';
            const nameOfPrimaryKey = '';
            const nameOfJsonColumn = 'column';
            await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);

        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
        }
    });
    it('create table api should fail for invalid primary key name', async function () {
        try {
            const tableName = 'hello';
            const nameOfPrimaryKey = null;
            const nameOfJsonColumn = 'column';
            await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);

        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
        }
    });
    it('create table api should fail for invalid primary key name should fail for invalid json column name',
        async function () {
            try {
                const tableName = 'hello';
                const nameOfPrimaryKey = 'test';
                const nameOfJsonColumn = '';
                await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);

            } catch (e) {
                expect(e).to.eql('please provide valid name for json column');
            }
        });
    it('create table api should fail for invalid primary key name should fail for invalid json column name',
        async function () {
            try {
                const tableName = 'hello';
                const nameOfPrimaryKey = 'test';
                const nameOfJsonColumn = null;
                await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);

            } catch (e) {
                expect(e).to.eql('please provide valid name for json column');
            }
        });
    it('create table api should fail for invalid primary key name greater than 64', async function () {
        try {
            const tableName = 'hello';
            const nameOfPrimaryKey = 'test';
            const nameOfJsonColumn = generateAValidString(65);
            await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
        }
    });

    it('createTable should fail when there is an external error', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, callback) {
            throw  new Error('error');
            callback('err', [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        // console.log(nameOfJsonColumn);
        try {
            const result = await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
        } catch (e) {
            const firstErrorLine = e.split('\n')[0];
            expect(firstErrorLine).to.eql('execution occurred while creating table Error: error');
        }
        mockedFunctions.connection.execute = saveExecute;
    });

    it('create table api should pass for valid data', async function () {
        const tableName = 'hello';
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const result = await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
        expect(result).to.eql(true);
    });
    it('create table api should fail when error happens', async function () {

        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, callback) {
            callback('err', [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';

        try {
            await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('err');
        }
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail for empty table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = '';
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        const valueForJsonColumn = '{}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;

    });
    it('put should fail for null table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = null;
        const nameOfPrimaryKey = 'abc';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail for number table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 1;
        const nameOfPrimaryKey = 'abc';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail for boolean table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = true;
        const nameOfPrimaryKey = 'abc';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail for null name of primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = null;
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail for empty ans name of primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = '';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail for number as primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 10;
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail for number if name of primary key length greater than 64', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = generateAValidString(65);
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail for number if name of primary key contain non alphanumeric', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = generateStringSequence('a', 40);
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail null primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = null;
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail empty primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail if length of primary key is greater than 255', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = generateStringSequence('a', 256);
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail json column name is null', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = null;
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail json column name is empty', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = '';
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail json column name is greater than 65', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = generateAValidString(66);
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail json column name is a number', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 10;
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail json column value is null', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = null;
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid JSON String column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail json value is string', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = 'hello';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid JSON String column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail json value is invalid json', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = '{hello}';
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid JSON String column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should pass for valid parameters', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, {
                ResultSetHeader: {
                    fieldCount: 0,
                    affectedRows: 0,
                    insertId: 0,
                    info: '',
                    serverStatus: 2,
                    warningStatus: 0
                }
            }, {});
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const x = {
            id: 'abc'
        };
        const valueForJsonColumn = x;

        const result = await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        expect(result).to.eql(true);

        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail if connection not initialise', async function () {
        close();
        let exceptionOccurred = false;
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, {
                ResultSetHeader: {
                    fieldCount: 0,
                    affectedRows: 0,
                    insertId: 0,
                    info: '',
                    serverStatus: 2,
                    warningStatus: 0
                }
            }, {});
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const x = {
            id: 'abc'
        };
        const valueForJsonColumn = JSON.stringify(x);

        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please call init before put');
        }
        expect(exceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('put should fail when there is error in connecting to MySQL', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            throw  new Error('error');
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const valueForJsonColumn = {};
        let isExceptionOccurred = false;
        try {
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueForJsonColumn);
        } catch (e) {
            const err = e.split('\n')[0];
            expect(err).to.eql('Exception occurred while writing to database Error: error');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if connection is not initialized', async function () {
        close();
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = '';
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e.toString()).to.eql('Error: Please call init before get');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;

    });

    it('get should fail for empty table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = '';
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;

    });

    it('get should fail for null table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = null;
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;

    });
    it('get should fail for number table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 10;
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail for boolean table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = true;
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail for Object table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = {};
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if table name gt 65', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = generateAValidString(65);
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if name of primary key is empty', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = generateAValidString(24);
        const nameOfPrimaryKey = '';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if name of primary key is null', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = generateAValidString(24);
        const nameOfPrimaryKey = null;
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if name of primary key is number', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = generateAValidString(24);
        const nameOfPrimaryKey = 10;
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if name of primary key is boolean', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = generateAValidString(24);
        const nameOfPrimaryKey = true;
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if name of primary key is Object', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = generateAValidString(24);
        const nameOfPrimaryKey = {};
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if length of name of primary key greater than 64', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = generateAValidString(64);
        const nameOfPrimaryKey = {};
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if nameOfJson column is empty', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = '';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if nameOfJson column is null', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = null;
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if nameOfJson column is number', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = 10;
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if nameOfJson column is boolean', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = true;
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if nameOfJson column is Object', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = {};
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if nameOfJson column is non alpha numeric', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = '*';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if nameOfJson column is has continues spaces', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = '     ';
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if length of nameOfJson column  greater than 64', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = generateAValidString(65);
        const primaryKey = '100';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if primary key is empty', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = generateAValidString(63);
        const primaryKey = '';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if primary key is null', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = generateAValidString(63);
        const primaryKey = null;
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if primary key is number', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = generateAValidString(63);
        const primaryKey = 10;
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if primary key is boolean', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = generateAValidString(63);
        const primaryKey = true;
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('get should fail if primary key is Object', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = 'customer_data';
        const primaryKey = {};
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if primary key has more than 255 valid characters', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = 'customer_data';
        const primaryKey = generateAValidString(256);
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should  give valid result if all parameters are correct', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null,
                [{'customer_data': JSON.stringify({customerData: 'bob'})}], []);
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = 'customer_data';
        const primaryKey = '100';
        const resultJson = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        const result = JSON.parse(resultJson);
        expect(result.customerData).to.eql('bob');
        mockedFunctions.connection.execute = saveExecute;
    });

    it('get should fail if there are any errors while executing get query', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            throw new Error('Error occurred while connecting');
        };
        const tableName = 'users';
        const nameOfPrimaryKey = 'id';
        const nameOfJsonColumn = 'customer_data';
        const primaryKey = '10';
        let isExceptionOccurred = false;
        try {
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        } catch (e) {

            expect(e.split('\n')[0]).to.eql('Exception occurred while getting data Error:' +
                ' Error occurred while connecting');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should fail if connection not initialise', async function () {
        close();
        let exceptionOccurred = false;
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, {
                ResultSetHeader: {
                    fieldCount: 0,
                    affectedRows: 0,
                    insertId: 0,
                    info: '',
                    serverStatus: 2,
                    warningStatus: 0
                }
            }, {});
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const x = {
            id: 'abc'
        };
        const valueForJsonColumn = JSON.stringify(x);

        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please call init before deleteKey');
        }
        expect(exceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should fail for empty table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = '';
        const nameOfPrimaryKey = 'test';
        const nameOfJsonColumn = 'customer';
        const primaryKey = '100';
        const valueForJsonColumn = '{}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;

    });
    it('deleteKey should fail for null table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = null;
        const nameOfPrimaryKey = 'abc';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should fail for number table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 1;
        const nameOfPrimaryKey = 'abc';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('put should fail for boolean table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = true;
        const nameOfPrimaryKey = 'abc';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('deleteKey should fail for null name of primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = null;
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('deleteKey should fail for empty ans name of primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = '';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should fail for number as primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 10;
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should fail for number if name of primary key length greater than 64', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = generateAValidString(65);
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should fail for number if name of primary key contain non alphanumeric', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = generateStringSequence('a', 40);
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '100q';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('please provide valid name for primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should fail null primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = null;
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('deleteKey should fail empty primary key', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = '';
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('deleteKey should fail if length of primary key is greater than 255', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer1';
        const primaryKey = generateStringSequence('a', 256);
        const valueForJsonColumn = '{x:[]}';
        let isExceptionOccurred = false;
        try {
            await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        } catch (e) {
            expect(e).to.eql('Please provide valid primary key');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteKey should pass for valid parameters', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, {
                ResultSetHeader: {
                    fieldCount: 0,
                    affectedRows: 0,
                    insertId: 0,
                    info: '',
                    serverStatus: 2,
                    warningStatus: 0
                }
            }, {});
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'customer';
        const primaryKey = generateStringSequence('a', 255);
        const x = {
            id: 'abc'
        };
        const valueForJsonColumn = x;

        const result = await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        expect(result).to.eql(true);

        mockedFunctions.connection.execute = saveExecute;
    });


    it('getFromNonIndex should fail null query value', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfJsonColumn = 'details';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, null);
        } catch (e) {
            expect(e).to.eql('please provide valid queryObject');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('getFromNonIndex should fail String query value', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfJsonColumn = 'details';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, 'hello');
        } catch (e) {
            expect(e).to.eql('please provide valid queryObject');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('getFromNonIndex should fail boolean query value', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfJsonColumn = 'details';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, true);
        } catch (e) {
            expect(e).to.eql('please provide valid queryObject');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('getFromNonIndex should fail number query value', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfJsonColumn = 'details';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, 10);
        } catch (e) {
            expect(e).to.eql('please provide valid queryObject');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('getFromNonIndex should fail invalid table Name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = '@';
        const nameOfJsonColumn = 'details';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, {
                id: 100
            });
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
    it('getFromNonIndex should fail empty object', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = '@';
        const nameOfJsonColumn = 'details';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, {});
        } catch (e) {
            expect(e).to.eql('please provide valid queryObject');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });


    it('getFromNonIndex should fail invalid nameOfJsonColumn', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'hello';
        const nameOfJsonColumn = null;
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, {
                id: '10'
            });
        } catch (e) {
            expect(e).to.eql('please provide valid name for json column');
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });


    it('getFromNonIndex should pass for valid parameters', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [{"details": {"Age": 100, "active": true, "lastName": "Alice"}}, {
                "details": {
                    "Age": 100,
                    "active": true,
                    "lastName": "Alice"
                }
            }], {});
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'details';
        const primaryKey = generateStringSequence('a', 255);
        const x = {
            id: 'abc'
        };
        const valueForJsonColumn = x;

        const result = await getFromNonIndex(tableName, nameOfJsonColumn, {
            id: 'abc'
        });
        expect(result.length).to.eql(2);
        for (let i = 0; i < result.length; i++) {
            expect(result[i].Age).to.eql(100);
            expect(result[i].active).to.eql(true);
            expect(result[i].lastName).to.eql('Alice');
        }

        mockedFunctions.connection.execute = saveExecute;
    });

    it('getFromNonIndex should pass when empty string', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], {});
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'details';
        const primaryKey = generateStringSequence('a', 255);

        const result = await getFromNonIndex(tableName, nameOfJsonColumn, {
            id: 'abc',
            lastName: 'Alice'
        });
        expect(result.length).to.eql(0);

        mockedFunctions.connection.execute = saveExecute;
    });

    it('getFromNonIndex should fail when exception occurs', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            throw new Error('external');
        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'details';
        const primaryKey = generateStringSequence('a', 255);

        let exceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, {
                id: 'abc'
            });
        } catch (e) {
            exceptionOccurred = true;

        }

        expect(exceptionOccurred).to.eql(true);

        mockedFunctions.connection.execute = saveExecute;
    });

    it('getFromNonIndex should fail if connection not initalized', async function () {
        try {
            close();
            const tableName = '';
            const nameOfPrimaryKey = 'id';
            const nameOfJsonColumn = 'column';
            await getFromNonIndex(tableName, nameOfJsonColumn, {});

        } catch (e) {
            expect(e.toString()).to.eql('Error: Please call init before getFromNonIndex');
        }
    });

    it('getFromNonIndex should fail when  error occurs', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback({}, [], {});

        };
        const tableName = 'hello';
        const nameOfPrimaryKey = 'bob';
        const nameOfJsonColumn = 'details';
        const primaryKey = generateStringSequence('a', 255);

        let exceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, nameOfJsonColumn, {
                id: 'abc'
            });
        } catch (e) {
            exceptionOccurred = true;

        }
        expect(exceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteTable should fail if connection not initialized', async function () {
        try {
            close();
            const tableName = 'customer';

            await deleteTable(tableName);

        } catch (e) {
            expect(e.toString()).to.eql('Error: Please call init before getFromNonIndex');
        }
    });

    it('deleteTable should fail invalid table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, callback) {
            callback(null, [], []);

        };
        const tableName = '@';

        let exceptionOccurred = false;
        try {
            await deleteTable(tableName);
        } catch (e) {
            expect(e).to.eql('please provide valid table name');
            exceptionOccurred = true;
        }
        expect(exceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteTable should pass for valid table name', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, callback) {
            callback(null, [], []);

        };
        const tableName = 'hello';

        let exceptionOccurred = false;
        try {
            const isSuccess = await deleteTable(tableName);
            expect(isSuccess).to.eql(true);
        } catch (e) {
            exceptionOccurred = true;
        }
        expect(exceptionOccurred).to.eql(false);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteTable should fail if exception occurs', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, callback) {
            throw new Error('exception occurred');

        };
        const tableName = 'hello';

        let exceptionOccurred = false;
        try {
            const isSuccess = await deleteTable(tableName);
        } catch (e) {
            exceptionOccurred = true;
        }
        expect(exceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });

    it('deleteTable should fail if error occurs', async function () {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, callback) {
            callback({}, [], []);

        };
        const tableName = 'hello';

        let exceptionOccurred = false;
        try {
            const isSuccess = await deleteTable(tableName);
        } catch (e) {
            exceptionOccurred = true;
        }
        expect(exceptionOccurred).to.eql(true);
        mockedFunctions.connection.execute = saveExecute;
    });
});
