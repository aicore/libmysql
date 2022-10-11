/*global describe, it, beforeEach*/
import mockedFunctions from '../setup-mocks.js';
import * as chai from 'chai';
import {
    createTable,
    get,
    put,
    init,
    close,
    deleteKey,
    getFromNonIndex,
    deleteTable,
    createIndexForJsonField,
    _createIndex,
    getFromIndex,
    update,
    createDataBase,
    deleteDataBase,
    mathAdd, query
} from "../../../src/utils/db.js";
import {
    JSON_COLUMN,
    DATA_TYPES
} from "../../../src/utils/constants.js";
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

    describe('create table tests', function () {
        it('create table api should fail  if connection not initialised', async function () {
            try {
                close();
                const tableName = 'customer';
                const jsonField = 'id';
                const dataType = 'INT';
                const isUnique = true;
                await createIndexForJsonField(tableName, jsonField, dataType, isUnique);

            } catch (e) {
                expect(e.toString()).to.eql('Please call init before createIndexForJsonField');
            }
        });

        it('create table api should fail  if connection not initialised', async function () {
            try {
                close();
                const tableName = '';
                await createTable(tableName);

            } catch (e) {
                expect(e.toString()).to.eql('Please call init before createTable');
            }
        });

        it('create table api should fail for invalid table name', async function () {
            try {
                const tableName = '';
                await createTable(tableName);

            } catch (e) {
                expect(e).to.eql('please provide valid table name in database.tableName format');
            }
        });
        it('create table api should fail for invalid table name', async function () {
            try {
                const tableName = null;
                await createTable(tableName);

            } catch (e) {
                expect(e).to.eql('please provide valid table name in database.tableName format');
            }
        });

        it('createTable should fail when there is an external error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw  new Error('error');
            };
            const tableName = 'a.hello';
            let isExceptionOccurred = false;
            try {
                await createTable(tableName);
            } catch (e) {
                isExceptionOccurred = true;
                const firstErrorLine = e.split('\n')[0];
                expect(firstErrorLine).to.eql('execution occurred while creating table Error: error');
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('create table api should pass for valid data', async function () {
            const tableName = 'test.hello';
            const result = await createTable(tableName);
            expect(result).to.eql(true);
        });
        it('create table api should fail when error happens', async function () {

            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback('err', [], []);
            };
            const tableName = 'test.hello';

            try {
                await createTable(tableName);
            } catch (e) {
                expect(e).to.eql('err');
            }
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('put API tests', function () {
        it('put should fail for empty table name', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = '';
            const document = '{}';
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                expect(e).to.eql('please provide valid table name in database.tableName format');
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
            const document = '{x:[]}';
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                expect(e).to.eql('please provide valid table name in database.tableName format');
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

            const document = '{x:[]}';
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                expect(e).to.eql('please provide valid table name in database.tableName format');
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
            const document = '{x:[]}';
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                expect(e).to.eql('please provide valid table name in database.tableName format');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('put for invalid document', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.hello';
            const document = null;
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                expect(e).to.eql('Please provide valid document');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('put should fail document  is string', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.hello';
            const document = 'hello';
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                expect(e).to.eql('Please provide valid document');
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
            const tableName = 'test.hello';
            const document = '{hello}';
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                expect(e).to.eql('Please provide valid document');
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
            const tableName = 'test.hello';
            const document = {
                id: 'abc'
            };

            const result = await put(tableName, document);
            expect(result.length).to.eql(32);

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
            const document = {
                id: 'abc'
            };

            try {
                await put(tableName, document);
            } catch (e) {
                exceptionOccurred = true;
                expect(e.toString()).to.eql('Please call init before put');
            }
            expect(exceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('put should fail when there is error in connecting to MySQL', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _values, _callback) {
                throw  new Error('error');
            };
            const tableName = 'test.hello';
            const document = {};
            let isExceptionOccurred = false;
            try {
                await put(tableName, document);
            } catch (e) {
                const err = e.split('\n')[0];
                expect(err).to.eql('Exception occurred while writing to database Error: error');
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
            const primaryKey = '100q';
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
    });

    describe('get API tests', function () {
        it('get should fail if connection is not initialized', async function () {
            close();
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = '';
            const documentId = '100';
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {
                expect(e.toString()).to.eql('Please call init before get');
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
            const documentId = '100';
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
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
            const documentId = '100';
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
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
            const documentId = '101';
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('please provide valid table name');
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
            const tableName = 'test.users';
            const documentId = '';
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
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
            const tableName = 'test.users';
            const documentId = null;
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
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
            const tableName = 'test.users';
            const documentId = 10;
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
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
            const tableName = 'test.users';
            const documentId = true;
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
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
            const tableName = 'test.users';
            const documentId = {};
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('get should fail if primary key has more than 128 valid characters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.users';
            const primaryKey = generateAValidString(129);
            let isExceptionOccurred = false;
            try {
                await get(tableName, primaryKey);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('get should  give valid result if all parameters are correct', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null,
                    [{'document': {customerData: 'bob'}}], []);
            };
            const tableName = 'test.users';
            const documentId = '100';
            const result = await get(tableName, documentId);
            expect(result.customerData).to.eql('bob');
            mockedFunctions.connection.execute = saveExecute;
        });

        it('get should fail if there are any errors while executing get query', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _values, _callback) {
                throw new Error('Error occurred while connecting');
            };
            const tableName = 'test.users';
            const documentId = '10';
            let isExceptionOccurred = false;
            try {
                await get(tableName, documentId);
            } catch (e) {

                expect(e.split('\n')[0]).to.eql('Exception occurred while getting data Error:' +
                    ' Error occurred while connecting');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('deleteKey API tests', function () {
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
            const documentId = generateStringSequence('a', 128);
            try {
                await deleteKey(tableName, documentId);
            } catch (e) {
                exceptionOccurred = true;
                expect(e.toString()).to.eql('Please call init before deleteKey');
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
            const documentId = '100';
            let isExceptionOccurred = false;
            try {
                await deleteKey(tableName, documentId);
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
            const primaryKey = '100q';
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
            const documentId = '100q';
            let isExceptionOccurred = false;
            try {
                await deleteKey(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('please provide valid table name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('deleteKey should fail for null primary key', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.hello';
            const documentId = null;
            let isExceptionOccurred = false;
            try {
                await deleteKey(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
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
            const tableName = 'test.hello';
            const documentId = '';
            let isExceptionOccurred = false;
            try {
                await deleteKey(tableName, documentId);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('deleteKey should fail if length of primary key is greater than 128', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.hello';
            const documentID = generateStringSequence('a', 129);
            let isExceptionOccurred = false;
            try {
                await deleteKey(tableName, documentID);
            } catch (e) {
                expect(e).to.eql('Please provide valid documentID');
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
            const tableName = 'test.hello';
            const documentId = generateStringSequence('a', 16);

            const result = await deleteKey(tableName, documentId);
            expect(result).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });
    });

    async function _validateFailsOnVarName(name, expectedErrorMessage, exceptionExpected = true) {
        const saveExecute = mockedFunctions.connection.execute;
        mockedFunctions.connection.execute = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'test.hello';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, JSON.parse(`{"${name}":"test"}`));
        } catch (e) {
            expectedErrorMessage = expectedErrorMessage || 'Exception occurred while getting data Error:' +
                ` Invalid filed name ${name}`;
            expect(e.split('\n')[0]).to.eql(expectedErrorMessage);
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(exceptionExpected);
        mockedFunctions.connection.execute = saveExecute;
    }

    describe('getFromNonIndex API tests', function () {
        it('getFromNonIndex should fail null query value', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'hello';
            let isExceptionOccurred = false;
            try {
                await getFromNonIndex(tableName, null);
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
            let isExceptionOccurred = false;
            try {
                await getFromNonIndex(tableName, 'hello');
            } catch (e) {
                expect(e).to.eql('please provide valid queryObject');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('getFromNonIndex should fail if key is not valid variable name', async function () {
            await _validateFailsOnVarName("AR#");
            await _validateFailsOnVarName("_Ar.");
            // field names of form a.y.x should also error out as `.` is not allowed within a variable name
            await _validateFailsOnVarName("Ar.x");

            // success case
            await _validateFailsOnVarName("Ar", null, false);
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
            let isExceptionOccurred = false;
            try {
                await getFromNonIndex(tableName, 10);
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
            let isExceptionOccurred = false;
            try {
                await getFromNonIndex(tableName, {
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


        it('getFromNonIndex should pass for valid parameters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [{"document": {"Age": 100, "active": true, "lastName": "Alice"}}, {
                    "document": {
                        "Age": 100,
                        "active": true,
                        "lastName": "Alice"
                    }
                }], {});
            };
            const tableName = 'test.hello';
            const result = await getFromNonIndex(tableName, {
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

        it('getFromNonIndex should return empty array if there are no results', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], {});
            };
            const tableName = 'test.hello';
            let isExceptionOccured = false;
            try {
                const result = await getFromNonIndex(tableName, {
                    id: 'abcNopeExist',
                    lastName: 'Alice'
                });
                expect(result).to.eql([]);
            } catch (e) {
                isExceptionOccured = true;
            }
            expect(isExceptionOccured).eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('getFromNonIndex should fail when exception occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _values, _callback) {
                throw new Error('external');
            };
            const tableName = 'hello';
            const nameOfJsonColumn = 'details';
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

        it('getFromNonIndex should fail if connection not initialized', async function () {
            try {
                close();
                const tableName = '';
                await getFromNonIndex(tableName, {});

            } catch (e) {
                expect(e.toString()).to.eql('Please call init before getFromNonIndex');
            }
        });

        it('getFromNonIndex should fail when  error occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback({}, [], {});

            };
            const tableName = 'hello';
            let exceptionOccurred = false;
            try {
                const queryObject = {
                    'Age': 100,
                    'lastName': 'Alice',
                    'location': {
                        'layout': {
                            'block': '1stblock'
                        }
                    }
                };
                await getFromNonIndex(tableName, queryObject);
            } catch (e) {
                exceptionOccurred = true;

            }
            expect(exceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('deleteTable API tests', function () {
        it('deleteTable should fail if connection not initialized', async function () {
            try {
                close();
                const tableName = 'customer';

                await deleteTable(tableName);

            } catch (e) {
                expect(e.toString()).to.eql('Please call init before getFromNonIndex');
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
            const tableName = 'test.hello';

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
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw new Error('exception occurred');

            };
            const tableName = 'hello';

            let exceptionOccurred = false;
            try {
                await deleteTable(tableName);
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
                await deleteTable(tableName);
            } catch (e) {
                exceptionOccurred = true;
            }
            expect(exceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('createIndexForJsonField API tests', function () {
        it('createIndexForJsonField should fail invalid tableName', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = '@';
            const jsonField = 'id';
            const dataType = DATA_TYPES.INT;
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
            } catch (e) {
                expect(e).to.eql('please provide valid table name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('createIndexForJsonField should fail invalid id', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            const jsonField = 10;
            const dataType = 'DATA_DATA_TYPES.INT';
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
            } catch (e) {
                expect(e).to.eql('please provide valid name for json field');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('createIndexForJsonField should fail invalid dataType', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            const jsonField = 'id.z.y';
            const dataType = null;
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
            } catch (e) {
                expect(e).to.eql('please provide valid  data type for json field');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('createIndexForJsonField should fail invalid jsonfield', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            const jsonField = 'id.z.@';
            const dataType = DATA_TYPES.INT;
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
            } catch (e) {
                expect(e).to.eql('please provide valid name for json field');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('createIndexForJsonField should pass for valid parameters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            const jsonField = 'id';
            const dataType = DATA_TYPES.INT;
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                const result = await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
                expect(result).to.eql(true);
            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('createIndexForJsonField should pass for valid parameters isUnique is false', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            const jsonField = 'id';
            const dataType = 'INT';
            const isUnique = false;
            let isExceptionOccurred = false;
            try {
                const result = await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
                expect(result).to.eql(true);
            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('createIndexForJsonField should fail if error occurred', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback('Error', [], []);
            };
            const tableName = 'test.customer';
            const jsonField = 'id';
            const dataType = 'INT';
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                const result = await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
                expect(result).to.eql(true);
            } catch (e) {
                expect(e).to.eql('Error');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('createIndexForJsonField should fail if exception occur while executing query', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw new Error('Exception');
            };
            const tableName = 'test.customer';
            const jsonField = 'id';
            const dataType = 'INT';
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                const result = await createIndexForJsonField(tableName, jsonField, dataType, isUnique);
                expect(result).to.eql(true);
            } catch (e) {
                expect(e.toString()).to.eql('Exception occurred while creating column for JSON field');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('_createIndex should fail if exception occur while executing query', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw new Error('Exception');
            };
            const tableName = 'customer';
            const nameOfJsonColumn = JSON_COLUMN;
            const jsonField = 'id';
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                const promise = new Promise(function (resolve, reject) {
                    _createIndex(resolve, reject, tableName, jsonField, isUnique);
                });
                await promise;
            } catch (e) {
                console.log(e);
                expect(e.toString()).to.eql('Exception occurred while creating index for JSON field');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('_createIndex should fail if exception occur while during call back', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback('Error', [], []);
            };
            const tableName = 'customer';
            const nameOfJsonColumn = JSON_COLUMN;
            const jsonField = 'id';
            const isUnique = true;
            let isExceptionOccurred = false;
            try {
                const promise = new Promise(function (resolve, reject) {
                    _createIndex(resolve, reject, tableName, nameOfJsonColumn, jsonField, isUnique);
                });
                await promise;
            } catch (e) {
                console.log(e);
                expect(e.toString()).to.eql('Error');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('getFromIndex API tests', function () {
        it('getFromIndex api should fail  if connection not initialised', async function () {
            try {
                close();
                const tableName = 'customer';
                const queryObject = {
                    'lastName': 'Alice',
                    'Age': 100
                };
                await getFromIndex(tableName, queryObject);

            } catch (e) {
                expect(e.toString()).to.eql('Please call init before findFromIndex');
            }
        });

        it('getFromIndex should fail null query object', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'hello';
            let isExceptionOccurred = false;

            try {
                await getFromIndex(tableName, null);
            } catch (e) {
                expect(e).to.eql('please provide valid queryObject');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('getFromIndex should fail empty query object', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'hello';
            let isExceptionOccurred = false;

            try {
                await getFromIndex(tableName, {});
            } catch (e) {
                expect(e).to.eql('please provide valid queryObject');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('getFromIndex should fail invalid table name', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = '@';
            let isExceptionOccurred = false;
            const queryObject = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                await getFromIndex(tableName, queryObject);
            } catch (e) {
                expect(e).to.eql('please provide valid table name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('getFromIndex should fail if error occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback("error", [], []);
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;
            const queryObject = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                await getFromIndex(tableName, queryObject);
            } catch (e) {
                expect(e).to.eql('error');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('getFromIndex should fail if exception  occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _values, _callback) {
                throw  new Error('error');
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;
            const queryObject = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                await getFromIndex(tableName, queryObject);
            } catch (e) {
                expect(e).to.eql('Exception occurred while querying index');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('getFromIndex should pass  for valid parameters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [{"document": {"Age": 100, "active": true, "lastName": "Alice"}}], []);
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;
            const queryObject = {
                'lastName': 'Alice',
                'Age': 100,
                'location': {
                    'layout': {
                        'block': '1stblock'
                    }

                }
            };
            try {
                const results = await getFromIndex(tableName, queryObject);
                expect(results[0].lastName).to.eql('Alice');
                expect(results[0].Age).to.eql(100);
                expect(results[0].active).to.eql(true);
                console.log(results);
            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('getFromIndex should return empty array if no data matches', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;
            const queryObject = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                const results = await getFromIndex(tableName, queryObject);
                expect(results.length).to.eql(0);
                console.log(results);
            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('update API tests', function () {
        it('update should fail if connection not initialized', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            close();
            const tableName = 'customer';
            const docId = '1000';
            let isExceptionOccurred = false;
            const document = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                await update(tableName, docId, document);

            } catch (e) {
                expect(e).to.eql('Please call init before update');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('update should fail if table name is invalid', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = '@';
            const docId = '1000';
            let isExceptionOccurred = false;
            const document = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                await update(tableName, docId, document);

            } catch (e) {
                expect(e).to.eql('please provide valid table name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('update should fail if document is invalid', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            const docId = '1000';
            let isExceptionOccurred = false;
            const document = null;
            try {
                await update(tableName, docId, document);

            } catch (e) {
                expect(e).to.eql('Please provide valid document');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('update should fail if error occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback("Error", [], []);
            };
            const tableName = 'test.customer';
            const docId = '1000';
            let isExceptionOccurred = false;
            const document = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                await update(tableName, docId, document);

            } catch (e) {
                expect(e).to.eql('Error');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('update should fail if exception occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                throw new Error('Error');
            };
            const tableName = 'test.customer';
            const docId = '1000';
            let isExceptionOccurred = false;
            const document = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                await update(tableName, docId, document);

            } catch (e) {
                expect(e.toString().split('\n')[0]).to.eql(
                    'Exception occurred while writing to database Error: Error');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('update should pass for valid parameters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            const docId = '1000';
            let isExceptionOccurred = false;
            const document = {
                'lastName': 'Alice',
                'Age': 100
            };
            try {
                const id = await update(tableName, docId, document);
                expect(id).to.eql(docId);

            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    it('check var char function', function () {
        let varchar = DATA_TYPES.VARCHAR();
        expect(varchar).to.eql('VARCHAR(50)');
        varchar = DATA_TYPES.VARCHAR(50);
        expect(varchar).to.eql('VARCHAR(50)');
    });

    describe('createDataBase API tests', function () {
        it('create database should pass for valid parameters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                const isSuccess = await createDataBase(database);
                expect(isSuccess).to.eql(true);

            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('create database should fail for invalid db name', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = '@';
            let isExceptionOccurred = false;
            try {
                await createDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Please provide valid data base name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('create database should fail for invalid empty db name', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = '';
            let isExceptionOccurred = false;
            try {
                await createDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Please provide valid data base name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('create database should fail if there is an error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback('Error occurred', [], []);
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                await createDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Error occurred');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('create database should fail if there is an external error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                throw new Error('Error Occurred');
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                await createDataBase(database);

            } catch (e) {
                expect(e.toString().split('\n')[0].trim())
                    .eql('exception occurred while creating database Error: Error Occurred');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('create database should fail if connection not initialized', async function () {
            close();
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                await createDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Please call init before createDataBase');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('deleteDataBase API tests', function () {
        it('delete database should pass for valid parameters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                const isSuccess = await deleteDataBase(database);
                expect(isSuccess).to.eql(true);

            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('delete database should fail for invalid db name', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = '@';
            let isExceptionOccurred = false;
            try {
                await deleteDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Please provide valid data base name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('delete database should fail for invalid empty db name', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = '';
            let isExceptionOccurred = false;
            try {
                await deleteDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Please provide valid data base name');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
        it('delete database should fail if there is an error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback('Error occurred', [], []);
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                await deleteDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Error occurred');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('delete database should fail if there is an external error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                throw new Error('Error Occurred');
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                await deleteDataBase(database);

            } catch (e) {
                expect(e.toString().split('\n')[0].trim())
                    .eql('execution occurred while deleting database Error: Error Occurred');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('delete database should fail if connection not initialized', async function () {
            close();
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const database = 'test';
            let isExceptionOccurred = false;
            try {
                await deleteDataBase(database);

            } catch (e) {
                expect(e.toString()).eql('Please call init before deleteDataBase');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('mathAdd API tests', function () {
        it('mathAdd should pass for valid params', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customers';
            const documentId = '100';
            const fieldsToIncrementMap = {
                age: 100,
                id: 10
            };

            const status = await mathAdd(tableName, documentId, fieldsToIncrementMap);
            expect(status).eql(true);
            mockedFunctions.connection.execute = saveExecute;

        });

        it('mathAdd should pass for valid params with single field', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customers';
            const documentId = '100';
            const fieldsToIncrementMap = {
                age: 100
            };

            const status = await mathAdd(tableName, documentId, fieldsToIncrementMap);
            expect(status).eql(true);
            mockedFunctions.connection.execute = saveExecute;

        });

        it('mathAdd should fail  for invalid table Name', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'customers';
            const documentId = '100';
            const fieldsToIncrementMap = {
                age: 100
            };
            let isExceptionOccurred = false;
            try {
                await mathAdd(tableName, documentId, fieldsToIncrementMap);
            } catch (e) {
                expect(e.toString()).eql('please provide valid table name');
                isExceptionOccurred = true;

            }
            expect(isExceptionOccurred).eql(true);
            mockedFunctions.connection.execute = saveExecute;

        });
        it('mathAdd should fail  for invalid docid', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customers';
            const documentId = 100;
            const fieldsToIncrementMap = {
                age: 100
            };
            let isExceptionOccurred = false;
            try {
                await mathAdd(tableName, documentId, fieldsToIncrementMap);
            } catch (e) {
                expect(e.toString()).eql('Please provide valid documentID');
                isExceptionOccurred = true;

            }
            expect(isExceptionOccurred).eql(true);
            mockedFunctions.connection.execute = saveExecute;

        });
        it('mathAdd should fail  for invalid field increment object', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, values, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customers';
            const documentId = '100';
            const fieldsToIncrementMap = {};
            let isExceptionOccurred = false;
            try {
                await mathAdd(tableName, documentId, fieldsToIncrementMap);
            } catch (e) {
                expect(e.toString()).eql('please provide valid increments for json filed');
                isExceptionOccurred = true;

            }
            expect(isExceptionOccurred).eql(true);
            mockedFunctions.connection.execute = saveExecute;

        });
        it('mathAdd should fail  for invalid field fieldsToIncrementMap contains non number fields',
            async function () {
                const saveExecute = mockedFunctions.connection.execute;
                mockedFunctions.connection.execute = function (sql, values, callback) {
                    callback(null, [], []);
                };
                const tableName = 'test.customers';
                const documentId = '100';
                const fieldsToIncrementMap = {
                    x: 10,
                    y: '100'
                };
                let isExceptionOccurred = false;
                try {
                    await mathAdd(tableName, documentId, fieldsToIncrementMap);
                } catch (e) {
                    expect(e.toString()).eql('increment can be done only with numerical values');
                    isExceptionOccurred = true;

                }
                expect(isExceptionOccurred).eql(true);
                mockedFunctions.connection.execute = saveExecute;

            });
        it('mathAdd should fail  for invalid field fieldsToIncrementMap contains when there is an error',
            async function () {
                const saveExecute = mockedFunctions.connection.execute;
                mockedFunctions.connection.execute = function (sql, values, callback) {
                    callback('Error', [], []);
                };
                const tableName = 'test.customers';
                const documentId = '100';
                const fieldsToIncrementMap = {
                    x: 10,
                    y: 100
                };
                let isExceptionOccurred = false;
                try {
                    await mathAdd(tableName, documentId, fieldsToIncrementMap);
                } catch (e) {
                    expect(e.toString()).eql('Error');
                    isExceptionOccurred = true;

                }
                expect(isExceptionOccurred).eql(true);
                mockedFunctions.connection.execute = saveExecute;

            });
        it('mathAdd should fail  if not initalized',
            async function () {
                close();
                const saveExecute = mockedFunctions.connection.execute;
                mockedFunctions.connection.execute = function (sql, values, callback) {
                    throw  new Error('Error');
                };
                const tableName = 'test.customers';
                const documentId = '100';
                const fieldsToIncrementMap = {
                    x: 10,
                    y: 100
                };
                let isExceptionOccurred = false;
                try {
                    await mathAdd(tableName, documentId, fieldsToIncrementMap);
                } catch (e) {
                    expect(e.toString().split('\n')[0].trim())
                        .eql('Please call init before get');
                    isExceptionOccurred = true;

                }
                expect(isExceptionOccurred).eql(true);
                mockedFunctions.connection.execute = saveExecute;

            });
        it('mathAdd should fail  for invalid field fieldsToIncrementMap contains when there is an External error',
            async function () {
                const saveExecute = mockedFunctions.connection.execute;
                mockedFunctions.connection.execute = function (sql, values, callback) {
                    throw  new Error('Error');
                };
                const tableName = 'test.customers';
                const documentId = '100';
                const fieldsToIncrementMap = {
                    x: 10,
                    y: 100
                };
                let isExceptionOccurred = false;
                try {
                    await mathAdd(tableName, documentId, fieldsToIncrementMap);
                } catch (e) {
                    expect(e.toString().split('\n')[0].trim())
                        .eql('Exception occurred while incrementing json fields Error: Error');
                    isExceptionOccurred = true;

                }
                expect(isExceptionOccurred).eql(true);
                mockedFunctions.connection.execute = saveExecute;

            });
    });

    describe('query API tests', function () {
        it('query api should fail  if connection not initialised', async function () {
            try {
                close();
                const tableName = 'customer';
                const queryStr = "lastname != 'hello'";
                await query(tableName, queryStr, []);

            } catch (e) {
                expect(e.toString()).to.eql('Please call init before findFromIndex');
            }
        });

        async function _validateQueryFail(queryString,
            tableName = 'hello',
            expectedException = 'please provide valid queryString') {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            let isExceptionOccurred = false;

            try {
                await query(tableName, queryString);
            } catch (e) {
                expect(e).to.eql(expectedException);
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        }

        it('query should fail null query string', async function () {
            await _validateQueryFail(null);
        });

        it('query should fail empty query string', async function () {
            await _validateQueryFail("");
        });

        it('query should fail if object passed as query string', async function () {
            await _validateQueryFail({});
        });

        it('query should fail invalid table name', async function () {
            await _validateQueryFail("a < 10", '@', 'please provide valid table name');
        });

        it('query should fail if error occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback("error", [], []);
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;

            try {
                await query(tableName, "a<10");
            } catch (e) {
                expect(e).to.eql('error');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('query should fail if exception  occurs', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw  new Error('error');
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;
            try {
                await query(tableName, "a<10");
            } catch (e) {
                expect(e).to.eql('Exception occurred while querying');
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(true);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('query should pass  for valid parameters', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [{"document": {"Age": 100, "active": true, "lastName": "Alice"}}], []);
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;
            try {
                const results = await query(tableName, "a<10");
                expect(results[0].lastName).to.eql('Alice');
                expect(results[0].Age).to.eql(100);
                expect(results[0].active).to.eql(true);
                console.log(results);
            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });

        it('query should return empty array if no data matches', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };
            const tableName = 'test.customer';
            let isExceptionOccurred = false;
            try {
                const results = await query(tableName, "a<10");
                expect(results.length).to.eql(0);
                console.log(results);
            } catch (e) {
                isExceptionOccurred = true;
            }
            expect(isExceptionOccurred).to.eql(false);
            mockedFunctions.connection.execute = saveExecute;
        });
    });
});
