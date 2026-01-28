/*global describe, it, beforeEach */
import mockedFunctions from '../setup-mocks.js';
import * as chai from 'chai';
import {
    init,
    close,
    listDatabases,
    listTables,
    getTableIndexes
} from "../../../src/utils/db.js";
import {getMySqlConfigs} from "@aicore/libcommonutils";

let expect = chai.expect;

describe('Unit tests for list APIs', function () {
    beforeEach(function () {
        close();
        init(getMySqlConfigs());
    });

    describe('listDatabases tests', function () {
        it('listDatabases should fail if connection not initialized', async function () {
            close();
            let isExceptionOccurred = false;
            try {
                await listDatabases();
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('Please call init before listDatabases');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('listDatabases should return array of database names on success', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [
                    {Database: 'information_schema'},
                    {Database: 'mysql'},
                    {Database: 'test'}
                ], []);
            };

            const databases = await listDatabases();
            expect(databases).to.eql(['information_schema', 'mysql', 'test']);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('listDatabases should return empty array when no databases', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };

            const databases = await listDatabases();
            expect(databases).to.eql([]);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('listDatabases should reject on database error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(new Error('Database error'), null, null);
            };

            let isExceptionOccurred = false;
            try {
                await listDatabases();
            } catch (e) {
                isExceptionOccurred = true;
                expect(e.message).to.eql('Database error');
            }
            expect(isExceptionOccurred).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('listDatabases should reject on execute exception', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw new Error('Execute exception');
            };

            let isExceptionOccurred = false;
            try {
                await listDatabases();
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.contain('Exception occurred while listing databases');
            }
            expect(isExceptionOccurred).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('listTables tests', function () {
        it('listTables should fail if connection not initialized', async function () {
            close();
            let isExceptionOccurred = false;
            try {
                await listTables('test');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('Please call init before listTables');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('listTables should fail for empty database name', async function () {
            let isExceptionOccurred = false;
            try {
                await listTables('');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('Please provide valid data base name');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('listTables should fail for null database name', async function () {
            let isExceptionOccurred = false;
            try {
                await listTables(null);
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('Please provide valid data base name');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('listTables should fail for invalid database name with special chars', async function () {
            let isExceptionOccurred = false;
            try {
                await listTables('test; DROP DATABASE test;--');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('Please provide valid data base name');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('listTables should return array of table names on success', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [
                    {Tables_in_test: 'customers'},
                    {Tables_in_test: 'orders'},
                    {Tables_in_test: 'products'}
                ], []);
            };

            const tables = await listTables('test');
            expect(tables).to.eql(['customers', 'orders', 'products']);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('listTables should return empty array when no tables', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(null, [], []);
            };

            const tables = await listTables('test');
            expect(tables).to.eql([]);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('listTables should reject on database error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (sql, callback) {
                callback(new Error('Database error'), null, null);
            };

            let isExceptionOccurred = false;
            try {
                await listTables('test');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e.message).to.eql('Database error');
            }
            expect(isExceptionOccurred).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('listTables should reject on execute exception', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw new Error('Execute exception');
            };

            let isExceptionOccurred = false;
            try {
                await listTables('test');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.contain('Exception occurred while listing tables');
            }
            expect(isExceptionOccurred).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });
    });

    describe('getTableIndexes tests', function () {
        it('getTableIndexes should fail if connection not initialized', async function () {
            close();
            let isExceptionOccurred = false;
            try {
                await getTableIndexes('test.customers');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('Please call init before getTableIndexes');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('getTableIndexes should fail for empty table name', async function () {
            let isExceptionOccurred = false;
            try {
                await getTableIndexes('');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('please provide valid table name in database.tableName format');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('getTableIndexes should fail for table name without database prefix', async function () {
            let isExceptionOccurred = false;
            try {
                await getTableIndexes('customers');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('please provide valid table name in database.tableName format');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('getTableIndexes should fail for invalid table name with special chars', async function () {
            let isExceptionOccurred = false;
            try {
                await getTableIndexes('test.customers; DROP TABLE customers;--');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.eql('please provide valid table name in database.tableName format');
            }
            expect(isExceptionOccurred).to.eql(true);
        });

        it('getTableIndexes should return indexes with JSON field mapping', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            let callCount = 0;

            mockedFunctions.connection.execute = function (sql, arg2, arg3) {
                callCount++;
                if (callCount === 1) {
                    // First call: SHOW INDEX FROM
                    const callback = arg2;
                    callback(null, [
                        {Key_name: 'PRIMARY', Column_name: 'documentID', Non_unique: 0, Seq_in_index: 1, Index_type: 'BTREE', Null: ''},
                        {Key_name: 'idx_col_abc123', Column_name: 'col_abc123', Non_unique: 1, Seq_in_index: 1, Index_type: 'BTREE', Null: 'YES'}
                    ], []);
                } else {
                    // Second call: information_schema query
                    const callback = arg3;
                    callback(null, [
                        {colName: 'col_abc123', genExpr: 'json_unquote(json_extract(`document`,_utf8mb4\'$.lastName\'))'}
                    ], []);
                }
            };

            const indexes = await getTableIndexes('test.customers');

            expect(indexes).to.have.lengthOf(2);

            expect(indexes[0].indexName).to.eql('PRIMARY');
            expect(indexes[0].columnName).to.eql('documentID');
            expect(indexes[0].jsonField).to.eql(null);
            expect(indexes[0].isUnique).to.eql(true);
            expect(indexes[0].isPrimary).to.eql(true);

            expect(indexes[1].indexName).to.eql('idx_col_abc123');
            expect(indexes[1].columnName).to.eql('col_abc123');
            expect(indexes[1].jsonField).to.eql('lastName');
            expect(indexes[1].isUnique).to.eql(false);
            expect(indexes[1].isPrimary).to.eql(false);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('getTableIndexes should handle nested JSON fields', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            let callCount = 0;

            mockedFunctions.connection.execute = function (sql, arg2, arg3) {
                callCount++;
                if (callCount === 1) {
                    const callback = arg2;
                    callback(null, [
                        {Key_name: 'idx_col_def456', Column_name: 'col_def456', Non_unique: 1, Seq_in_index: 1, Index_type: 'BTREE', Null: 'YES'}
                    ], []);
                } else {
                    const callback = arg3;
                    callback(null, [
                        {colName: 'col_def456', genExpr: 'json_unquote(json_extract(`document`,_utf8mb4\'$.address.city\'))'}
                    ], []);
                }
            };

            const indexes = await getTableIndexes('test.customers');

            expect(indexes).to.have.lengthOf(1);
            expect(indexes[0].jsonField).to.eql('address.city');

            mockedFunctions.connection.execute = saveExecute;
        });

        it('getTableIndexes should return empty array when no indexes', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            let callCount = 0;

            mockedFunctions.connection.execute = function (sql, arg2, arg3) {
                callCount++;
                if (callCount === 1) {
                    const callback = arg2;
                    callback(null, [], []);
                } else {
                    const callback = arg3;
                    callback(null, [], []);
                }
            };

            const indexes = await getTableIndexes('test.customers');
            expect(indexes).to.eql([]);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('getTableIndexes should reject on first query error', async function () {
            const saveExecute = mockedFunctions.connection.execute;

            mockedFunctions.connection.execute = function (sql, callback) {
                callback(new Error('Index query error'), null, null);
            };

            let isExceptionOccurred = false;
            try {
                await getTableIndexes('test.customers');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e.message).to.eql('Index query error');
            }
            expect(isExceptionOccurred).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('getTableIndexes should reject on second query error', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            let callCount = 0;

            mockedFunctions.connection.execute = function (sql, arg2, arg3) {
                callCount++;
                if (callCount === 1) {
                    const callback = arg2;
                    callback(null, [{Key_name: 'PRIMARY', Column_name: 'documentID', Non_unique: 0, Seq_in_index: 1, Index_type: 'BTREE', Null: ''}], []);
                } else {
                    const callback = arg3;
                    callback(new Error('Column query error'), null, null);
                }
            };

            let isExceptionOccurred = false;
            try {
                await getTableIndexes('test.customers');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e.message).to.eql('Column query error');
            }
            expect(isExceptionOccurred).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });

        it('getTableIndexes should reject on execute exception', async function () {
            const saveExecute = mockedFunctions.connection.execute;
            mockedFunctions.connection.execute = function (_sql, _callback) {
                throw new Error('Execute exception');
            };

            let isExceptionOccurred = false;
            try {
                await getTableIndexes('test.customers');
            } catch (e) {
                isExceptionOccurred = true;
                expect(e).to.contain('Exception occurred while getting table indexes');
            }
            expect(isExceptionOccurred).to.eql(true);

            mockedFunctions.connection.execute = saveExecute;
        });
    });
});
