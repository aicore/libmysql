// Testing framework: Mocha , assertion style: chai
// See https://mochajs.org/#getting-started on how to write tests
// Use chai for BDD style assertions (expect, should etc..). See move here: https://www.chaijs.com/guide/styles/#expect

// Mocks and spies: sinon
// if you want to mock/spy on fn() for unit tests, use sinon. refer docs: https://sinonjs.org/

// Note on coverage suite used here:
// we use c8 for coverage https://github.com/bcoe/c8. Its reporting is based on nyc, so detailed docs can be found
// here: https://github.com/istanbuljs/nyc ; We didn't use nyc as it do not yet have ES module support
// see: https://github.com/digitalbazaar/bedrock-test/issues/16 . c8 is drop replacement for nyc coverage reporting tool

// remove integration tests if you don't have them.
// jshint ignore: start
/*global describe, it, after, before, beforeEach. afterEach */

import * as chai from 'chai';
import {getMySqlConfigs} from './setupIntegTest.js';
import LibMySql from "../../src/index.js";

const createIndexForJsonField = LibMySql.createIndexForJsonField;
const createTable = LibMySql.createTable;
const deleteKey = LibMySql.deleteKey;
const deleteTable = LibMySql.deleteTable;
const getFromIndex = LibMySql.getFromIndex;
const get = LibMySql.get;
const getFromNonIndex = LibMySql.getFromNonIndex;
const put = LibMySql.put;
const update = LibMySql.update;
const DATA_TYPES = LibMySql.DATA_TYPES;

import {init, close, createDataBase, deleteDataBase, mathAdd, query, deleteDocuments, listDatabases, listTables, getTableIndexes} from "../../src/utils/db.js";
import {isObjectEmpty} from "@aicore/libcommonutils";
import * as crypto from "crypto";

let expect = chai.expect;

//const tableName = 'test.customers';
let tableName = '';
const database = 'test';
describe('Integration: libMySql', function () {
    after(async function () {
        await LibMySql.deleteDataBase(database);
        close();
    });
    before(async function () {
        const configs = await getMySqlConfigs();
        console.log(`${JSON.stringify(configs)}`);
        tableName = database + '.customers';
        init(configs);
        await createDataBase(database);
    });

    beforeEach(async function () {

        await createTable(tableName);
    });
    afterEach(async function () {
        await deleteTable(tableName);
    });

    it('should create table add data and get data', async function () {
        const document = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        const docId = await put(tableName, document);
        const results = await get(tableName, docId);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);
        expect(results.documentId.length).gt(0);

        let isExceptionOccured = false;
        try {
            await get(tableName, '1');
        } catch (e) {
            isExceptionOccured = true;
            expect(e.toString()).eql('unable to find document for given documentId');
        }
        expect(isExceptionOccured).eql(true);


        // delete key
        await deleteKey(tableName, docId);
        isExceptionOccured = false;
        try {
            await get(tableName, docId);
        } catch (e) {
            isExceptionOccured = true;
            expect(e.toString()).eql('unable to find document for given documentId');
        }
        expect(isExceptionOccured).eql(true);


    });
    it('should be able to add nested objects to document', async function () {
        const document = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true,
            'location': {
                'city': 'Banglore',
                'state': 'Karnataka'

            }
        };
        const docId = await put(tableName, document);
        const results = await get(tableName, docId);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);
        expect(results.location.city).to.eql(document.location.city);
        expect(results.location.state).to.eql(document.location.state);
        const queryObject = {
            'lastName': 'Alice',
            'Age': 100
        };
        // delete key
        await deleteKey(tableName, docId);
        let isExceptionOccurred = false;
        try {
            await get(tableName, docId);
        } catch (e) {
            isExceptionOccurred = true;
            expect(e.toString()).eql('unable to find document for given documentId');
        }
        expect(isExceptionOccurred).eql(true);

    });

    it('get should throw exception if data not present', async function () {

        const docId = crypto.randomBytes(16).toString('hex');
        let isExceptionOccurred = false;
        try {
            await get(tableName, docId);
        } catch (e) {
            isExceptionOccurred = true;
            expect(e.toString()).eql('unable to find document for given documentId');
        }
        expect(isExceptionOccurred).eql(true);
    });
    it('get should throw exception table is not present', async function () {
        let exceptionOccurred = false;
        try {
            const docId = crypto.randomBytes(16).toString('hex');
            await get(database + '.HELLO', docId);

        } catch (e) {
            exceptionOccurred = true;
            expect(e.code).to.eql('ER_NO_SUCH_TABLE');
        }
        expect(exceptionOccurred).to.eql(true);
    });

    it('put should throw exception table is not present', async function () {
        let exceptionOccurred = false;
        try {

            const document = {
                'lastName': 'Alice',
                'Age': 100,
                'active': true
            };
            await put(database + '.hello', document);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.code).to.eql('ER_NO_SUCH_TABLE');
        }
        expect(exceptionOccurred).to.eql(true);
    });
    it('100 writes followed by read', async function () {
        const results = await testReadWrite(100);
        await deleteData(results);
    });

    it('1000 writes followed by read', async function () {
        const results = await testReadWrite(1000);
        await deleteData(results);
    });
    it('1500 writes followed by read', async function () {
        const results = await testReadWrite(1500);
        await deleteData(results);

    });

    async function deleteData(results) {
        const deletePromises = [];
        for (let i = 0; i < results.documentIds.length; i++) {
            let deletePromise = deleteKey(results.tableName, results.documentIds[i]);
            deletePromises.push(deletePromise);
        }
        const deleteReturns = await Promise.all(deletePromises);
        expect(deleteReturns.length).to.eql(deletePromises.length);
        for (let i = 0; i < results.documentIds.length; i++) {
            expect(deleteReturns[i]).to.eql(true);
        }
    }


    it('should be able to update data', async function () {

        let document = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        const docId1 = await put(tableName, document);
        let results = await get(tableName, docId1);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);

        document = {
            'lastName': 'Alice1',
            'Age': 140,
            'active': true
        };
        await update(tableName, docId1, document);
        results = await get(tableName, docId1);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);

        await deleteKey(tableName, docId1);


    });

    it('should delete data only if condition is satisfied', async function () {

        let document = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        const docId1 = await put(tableName, document);
        let results = await get(tableName, docId1);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);

        // no delete with condition that doesn't match
        let status = await deleteKey(tableName, docId1, "$.Age=200");
        expect(status).to.be.true;
        // document should be present
        results = await get(tableName, docId1);
        expect(results.lastName).to.eql(document.lastName);

        // now delete with valid condition
        status = await deleteKey(tableName, docId1, "$.Age=100");
        expect(status).to.be.true;
        // document should not be present
        let err;
        try{
            await get(tableName, docId1);
        } catch (e) {
            err = e;
        }
        expect(err).to.exist;

    });

    it('should be able to update data on condition', async function () {

        let document = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        const docId1 = await put(tableName, document);
        let results = await get(tableName, docId1);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);

        let document2 = {
            'lastName': 'Alice1',
            'Age': 140,
            'active': true
        };
        // this update should fail as condition not satisfied
        let exceptionOccurred = false;
        try{
            await update(tableName, docId1, document2, "NOT($.Age = 100)");
        } catch(e) {
            exceptionOccurred = true;
        }
        expect(exceptionOccurred).to.be.true;
        results = await get(tableName, docId1);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);

        // this update should pass as condition satisfied
        await update(tableName, docId1, document2, "$.Age = 100");
        results = await get(tableName, docId1);
        expect(results.lastName).to.eql(document2.lastName);
        expect(results.Age).to.eql(document2.Age);
        expect(results.active).to.eql(document2.active);

        await deleteKey(tableName, docId1);


    });

    it('should be able to do scan and return results from database', async function () {
        const numberOfEntries = 1500;
        const results = await testReadWrite(numberOfEntries);
        let scanResults = await getFromNonIndex(results.tableName, {
            'lastName': 'Alice',
            'Age': 100
        });
        expect(scanResults.length).to.eql(1000);
        for (let i = 0; i < scanResults.length; i++) {
            expect(scanResults[i].Age).to.eql(100);
            expect(scanResults[i].active).to.eql(true);
            expect(scanResults[i].lastName).to.eql('Alice');
            expect(scanResults[i].documentId.length).gt(0);
        }
        scanResults = await getFromNonIndex(results.tableName);
        expect(scanResults.length).eql(1000);
        for (let i = 0; i < scanResults.length; i++) {
            expect(scanResults[i].Age).to.eql(100);
            expect(scanResults[i].active).to.eql(true);
            expect(scanResults[i].lastName).to.eql('Alice');
            expect(scanResults[i].documentId.length).gt(0);
        }
        await deleteData(results);
    });

    it('should be able to do paginated scan and return results from database', async function () {
        const numberOfEntries = 150;
        const results = await testReadWrite(numberOfEntries);
        let scanResults = await getFromNonIndex(results.tableName, {
            'lastName': 'Alice',
            'Age': 100
        }, {
            pageOffset: 0,
            pageLimit: 10
        });
        expect(scanResults.length).to.eql(10);
        let lastResultCounter = scanResults[scanResults.length-1].counter;
        scanResults = await getFromNonIndex(results.tableName, {}, {
            pageOffset: 9,
            pageLimit: 10
        });
        let firstResultCounter = scanResults[0].counter;
        expect(scanResults.length).eql(10);
        expect(lastResultCounter).to.eql(firstResultCounter);
        await deleteData(results);
    });

    it('should be able to do scan and return results from database for nested objects', async function () {
        const numberOfEntries = 100;
        const results = await testReadWrite(numberOfEntries);
        const queryObject = {
            'Age': 100,
            'lastName': 'Alice',
            'location': {
                'layout': {
                    'block': '1stblock'
                }

            }
        };
        const scanResults = await getFromNonIndex(results.tableName, queryObject);
        expect(scanResults.length).to.eql(numberOfEntries);
        for (let i = 0; i < scanResults.length; i++) {
            expect(scanResults[i].Age).to.eql(100);
            expect(scanResults[i].active).to.eql(true);
            expect(scanResults[i].lastName).to.eql('Alice');
            expect(scanResults[i].documentId.length).gt(0);
        }
        await deleteData(results);
    });


    it('delete table should pass if table does not exit', async function () {
        const isSuccess = await deleteTable(database + '.hello');
        expect(isSuccess).to.eql(true);

    });
    it('create and validate Index should pass', async function () {
        const numberOfEntries = 1000;
        const results = await testReadWrite(numberOfEntries);
        let isSuccess = await createIndexForJsonField(tableName, 'lastName', DATA_TYPES.VARCHAR(50),
            false);
        expect(isSuccess).to.eql(true);
        isSuccess = await createIndexForJsonField(tableName, 'Age', DATA_TYPES.INT, false);
        expect(isSuccess).to.eql(true);
        const queryResults = await getFromIndex(tableName, {
            'lastName': 'Alice',
            'Age': 100
        });
        expect(queryResults.length).to.eql(numberOfEntries);
        queryResults.forEach(result => {
            expect(result.lastName).to.eql('Alice');
            expect(result.Age).to.eql(100);
            expect(result.active).to.eql(true);
            expect(result.documentId.length).gt(0);
        });
        await deleteData(results);
    });
    it('create and validate Index should pass', async function () {
        const numberOfEntries = 1000;
        const results = await testReadWrite(numberOfEntries);
        let isSuccess = await createIndexForJsonField(tableName, 'userId', DATA_TYPES.VARCHAR(50),
            false);
        expect(isSuccess).to.eql(true);
        isSuccess = await createIndexForJsonField(tableName, 'Age', DATA_TYPES.INT, false);
        expect(isSuccess).to.eql(true);
        const queryResults = await getFromIndex(tableName, {
            'userId': 'ABCD',
            'Age': 100
        });
        expect(queryResults.length).to.eql(numberOfEntries);
        queryResults.forEach(result => {
            expect(result.lastName).to.eql('Alice');
            expect(result.userId).to.eql("ABCD");
            expect(result.Age).to.eql(100);
            expect(result.active).to.eql(true);
            expect(result.documentId.length).gt(0);
        });
        await deleteData(results);
    });

    it('create and validate Index return empty list if nothing matches', async function () {
        let isSuccess = await createIndexForJsonField(tableName, 'location.layout.block', DATA_TYPES.VARCHAR(50),
            false);
        expect(isSuccess).to.eql(true);
        isSuccess = await createIndexForJsonField(tableName, 'Age', DATA_TYPES.INT, false);
        expect(isSuccess).to.eql(true);
        let isExceptionOccured = false;
        try {
            let documents = await getFromIndex(tableName, {
                'Age': 100,
                'location': {
                    'layout': {
                        'block': '1stblock'
                    }

                }
            });
            expect(documents).to.eql([]);
        } catch (e) {
            isExceptionOccured = true;
        }
        expect(isExceptionOccured).eql(false);
    });

    it('create a unique non null json column should pass', async function () {
        let isSuccess = await createIndexForJsonField(tableName, 'location.layout.block', DATA_TYPES.VARCHAR(50),
            true, true);
        expect(isSuccess).to.eql(true);
        isSuccess = await createIndexForJsonField(tableName, 'Age', DATA_TYPES.INT, false, true);
        expect(isSuccess).to.eql(true);
        const docId = await put(tableName, {
            'lastName': 'Alice',
            'Age': 100,
            'active': true,
            'location': {
                'city': 'Banglore',
                'state': 'Karnataka',
                'layout': {
                    'block': '1stblock'
                }
            }
        });
        expect(docId.length).to.eql(32);

    });

    it('create a unique non null json column should fail if there in duplicate insert', async function () {
        let isSuccess = await createIndexForJsonField(tableName, 'location.layout.block', DATA_TYPES.VARCHAR(50),
            true, true);
        expect(isSuccess).to.eql(true);
        await createIndexForJsonField(tableName, 'Age', DATA_TYPES.INT, false, true);
        const docId = await put(tableName, {
            'lastName': 'Alice',
            'Age': 100,
            'active': true,
            'location': {
                'city': 'Banglore',
                'state': 'Karnataka',
                'layout': {
                    'block': '1stblock'
                }
            }
        });
        expect(docId.length).to.eql(32);
        let isExceptionOccurred = false;
        try {
            await put(tableName, {
                'lastName': 'Alice',
                'Age': 100,
                'active': true,
                'location': {
                    'city': 'Banglore',
                    'state': 'Karnataka',
                    'layout': {
                        'block': '1stblock'
                    }
                }
            });
        } catch (e) {
            isExceptionOccurred = true;
            expect(e.toString().split('\n')[0])
                .to.contains("Error: Duplicate entry '1stblock' for key 'customers.");
        }
        expect(isExceptionOccurred).to.eql(true);

    });
    it('create a unique non null json column and try to insert null field', async function () {
        let isSuccess = await createIndexForJsonField(tableName, 'location.layout.block', DATA_TYPES.VARCHAR(50),
            true, true);
        expect(isSuccess).to.eql(true);
        isSuccess = await createIndexForJsonField(tableName, 'Age', DATA_TYPES.INT, false, true);
        expect(isSuccess).to.eql(true);
        let isExceptionOccurred = false;
        try {
            await put(tableName, {
                'lastName': 'Alice',
                'Age': 100,
                'active': true,
                'location': {
                    'city': 'Banglore',
                    'state': 'Karnataka'
                }
            });
        } catch (e) {
            isExceptionOccurred = true;
            expect(e.toString().split('\n')[0])
                .to.contains("cannot be null");
        }
        expect(isExceptionOccurred).to.eql(true);

    });
    it('create and delete database should be successful', async function () {
        const dbName = 'hello';
        const createDatabaseStatus = await createDataBase(dbName);
        expect(createDatabaseStatus).eql(true);
        const deleteDatabaseStatus = await deleteDataBase(dbName);
        expect(deleteDatabaseStatus).eql(true);
    });

    it('create same database twice should fail', async function () {
        const dbName = 'hello';
        try {
            let createDatabaseStatus = await createDataBase(dbName);
            expect(createDatabaseStatus).eql(true);
            await createDataBase(dbName);

        } catch (e) {
            expect(e.toString().trim()).eql('Error: Can\'t create database \'hello\'; database exists');
            console.log(e.toString());
        } finally {
            await deleteDataBase(dbName);
        }

    });
    it('delete non exiting database should fail', async function () {
        const dbName = 'hello';
        try {
            await deleteDataBase(dbName);

        } catch (e) {
            expect(e.toString().trim()).eql('Error: Can\'t drop database \'hello\'; database doesn\'t exist');
            console.log(e.toString());
        }
    });
    it('should be able to increment json field', async function () {
        const docId = await put(tableName, {age: 10, total: 100});
        let incStatus = await mathAdd(tableName, docId, {
            age: 2,
            total: 100
        });
        expect(incStatus).eql(true);
        let modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.age).eql(12);
        expect(modifiedDoc.total).eql(200);
        incStatus = await mathAdd(tableName, docId, {
            age: 1
        });
        expect(incStatus).eql(true);
        modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.age).eql(13);
        expect(modifiedDoc.total).eql(200);
        incStatus = await mathAdd(tableName, docId, {
            age: -2,
            total: -300
        });
        expect(incStatus).eql(true);
        modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.age).eql(11);
        expect(modifiedDoc.total).eql(-100);
        incStatus = await mathAdd(tableName, docId, {
            count: -2
        });
        expect(incStatus).eql(true);
        modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.age).eql(11);
        expect(modifiedDoc.total).eql(-100);
        expect(modifiedDoc.count).eql(-2);
    });
    it('should be able to increment json field with condition that passes', async function () {
        // First create a document with initial values
        const docId = await put(tableName, {age: 10, total: 100, active: true});

        // Test increment with condition that should pass
        let incStatus = await mathAdd(tableName, docId, {
            age: 2,
            total: 100
        }, "$.age=10");

        expect(incStatus).eql(true);

        // Verify that the increments were applied
        let modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.age).eql(12);
        expect(modifiedDoc.total).eql(200);

        // Test with a different condition format
        incStatus = await mathAdd(tableName, docId, {
            age: 1
        }, "$.total>150");

        expect(incStatus).eql(true);

        // Verify the increment was applied
        modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.age).eql(13);
        expect(modifiedDoc.total).eql(200);
    });

    it('should not increment json field when condition fails', async function () {
        // First create a document with initial values
        const docId = await put(tableName, {age: 20, total: 200, active: true});

        // Test increment with condition that should fail
        try {
            await mathAdd(tableName, docId, {
                age: 5,
                total: 50
            }, "$.age>50");

            // Should not reach here
            expect.fail('Should have thrown an error when condition failed');
        } catch (err) {
            expect(err).to.eql('Not updated - condition failed or unable to find documentId');
        }

        // Verify that the document wasn't modified
        let doc = await get(tableName, docId);
        expect(doc.age).eql(20);
        expect(doc.total).eql(200);
    });

    it('should handle complex conditions for json increment', async function () {
        // First create a document with initial values
        const docId = await put(tableName, {
            age: 25,
            visits: 10,
            balance: 500,
            status: {
                premium: true,
                lastLogin: 1000
            }
        });

        // Test increment with a more complex condition
        let incStatus = await mathAdd(tableName, docId, {
            visits: 1,
            balance: 100
        }, "$.age>20 && $.balance<1000");

        expect(incStatus).eql(true);

        // Verify that increments were applied
        let modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.visits).eql(11);
        expect(modifiedDoc.balance).eql(600);

        // Test with negative increments and condition
        incStatus = await mathAdd(tableName, docId, {
            balance: -50
        }, "$.visits>10");

        expect(incStatus).eql(true);

        // Verify that increment was applied
        modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.balance).eql(550);

        // Test with nested field in condition
        incStatus = await mathAdd(tableName, docId, {
            visits: 2
        }, "$.status.lastLogin=1000");

        expect(incStatus).eql(true);

        // Verify that increment was applied
        modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.visits).eql(13);
    });

    it('should handle new fields and field initialization with conditions', async function () {
        // First create a document with initial values
        const docId = await put(tableName, {score: 100, level: 5});

        // Test increment of non-existent field with condition
        let incStatus = await mathAdd(tableName, docId, {
            points: 10
        }, "$.level>3");

        expect(incStatus).eql(true);

        // Verify that the new field was created with the increment value
        let modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.points).eql(10); // Should start at 0 and increment by 10
        expect(modifiedDoc.level).eql(5);   // Should be unchanged

        // Test with multiple fields including non-existent ones
        incStatus = await mathAdd(tableName, docId, {
            score: 50,
            coins: 100
        }, "$.points=10");

        expect(incStatus).eql(true);

        // Verify the changes
        modifiedDoc = await get(tableName, docId);
        expect(modifiedDoc.score).eql(150);
        expect(modifiedDoc.coins).eql(100);
        expect(modifiedDoc.points).eql(10); // Unchanged since it was used in condition
    });
    it('basic query test should pass', async function () {
        const docId = await put(tableName, {age: 10, total: 100});
        const results = await query(tableName, '$.age = 10');
        expect(results[0].documentId).eql(docId);
        expect(results[0].age).eql(10);
        expect(results[0].total).eql(100);
    });
    it('query with nested object', async function () {
        const docId = await put(tableName, {
            'lastName': 'Alice',
            'Age': 100,
            'active': true,
            'location': {
                'city': 'Banglore',
                'state': 'Karnataka'
            }
        });
        const results = await query(tableName, "$.location.city  = 'Banglore'");
        expect(results[0].documentId).eql(docId);
        expect(results[0].Age).eql(100);
        expect(results[0].location.state).eql('Karnataka');

    });

    async function putDocs(numDocs, run) {
        let promises = [];
        for(let i=0; i<numDocs; i++){
            promises.push(put(tableName, {
                'lastName': 'Alice',
                'Age': i,
                'active': true,
                'run': run,
                'location': {
                    'city': `Banglore-${i}`,
                    'state': 'Karnataka'
                }
            }));
        }
        await Promise.all(promises);
    }

    it('query should return all results', async function () {
        await putDocs(1500, 1);
        const results = await query(tableName, "$.location.city  LIKE 'Banglore-1_' && $.run = 1");
        expect(results.length).eql(10);
        for(let i=0; i < results.length; i++){
            expect(results[i].location.city === `Banglore-${results[i].Age}`).to.be.true;
        }
    });
    it('large 3600 query test', async function () {
        await putDocs(1500, 2);
        let promises = [];
        let numIterations = 9, maxPasses = 400; // 3600 queries
        for(let j=0; j<maxPasses; j++){
            for(let i=1; i<=numIterations; i++) {
                promises.push(query(tableName, `$.location.city  LIKE 'Banglore-${i}_' && $.run = 2`));
            }
        }
        const results = await Promise.all(promises);
        expect(results.length).eql(maxPasses*numIterations);
        for(let i=0; i < results.length; i++){
            expect(results[i].length === 10 && results[i][0].run === 2).to.be.true;
        }
    });
    it('query with index should pass', async function () {

        const createIndexStatus = await createIndexForJsonField(tableName, 'location.city', DATA_TYPES.VARCHAR(), false, true);
        expect(createIndexStatus).eql(true);

        const docId = await put(tableName, {
            'lastName': 'Alice',
            'Age': 100,
            'active': true,
            'location': {
                'city': 'Banglore',
                'state': 'Karnataka'
            }
        });
        const results = await query(tableName, "$.location.city  = 'Banglore' AND $.location.state = 'Karnataka'", ['location.city']);
        expect(results[0].documentId).eql(docId);
        expect(results[0].Age).eql(100);
        expect(results[0].location.state).eql('Karnataka');

    });

    it('query boolean should be treated as false', async function () {

        const createIndexStatus = await createIndexForJsonField(tableName, 'location.city', DATA_TYPES.VARCHAR(), false, true);
        expect(createIndexStatus).eql(true);

        const docId = await put(tableName, {
            'lastName': 'Alice',
            'Age': 100,
            'active': false,
            'location': {
                'city': 'Banglore',
                'state': 'Karnataka'
            }
        });
        const results = await query(tableName, "$.active  = 0", ['location.city']);
        expect(results[0].documentId).eql(docId);
        expect(results[0].Age).eql(100);
        expect(results[0].location.state).eql('Karnataka');

    });

    // delete documents tests
    it('deleteDocuments without index field should delete single document', async function () {
        await populateTestTable(100);
        let results = await query(tableName, "$.counter  = 10");
        expect(results.length).eql(1);

        let deletedDocCount = await deleteDocuments(tableName, "$.counter  = 10");
        expect(deletedDocCount).eql(1);
        results = await query(tableName, "$.counter  = 10");
        expect(results.length).eql(0);

        // deleting again should delete no documents as there are none
        deletedDocCount = await deleteDocuments(tableName, "$.counter  = 10");
        expect(deletedDocCount).eql(0);
    });
    it('deleteDocuments without index field should delete multiple document', async function () {
        await populateTestTable(100);
        let results = await query(tableName, "$.counter  >= 10 AND $.counter  <= 27");
        expect(results.length).eql(18);

        let deletedDocCount = await deleteDocuments(tableName, "$.counter  >= 10 AND $.counter  <= 27");
        expect(deletedDocCount).eql(18);
        results = await query(tableName, "$.counter  >= 10 AND $.counter  <= 27");
        expect(results.length).eql(0);

        // deleting again should delete no documents as there are none
        deletedDocCount = await deleteDocuments(tableName, "$.counter  >= 10 AND $.counter  <= 27");
        expect(deletedDocCount).eql(0);

        // count remaining documents to verify nothing else gets deleted
        results = await query(tableName, "$.Age  = 100"); // all has age 100
        expect(results.length).eql(82);
    });
    it('deleteDocuments with index field should delete single document', async function () {
        const createIndexStatus = await createIndexForJsonField(tableName, 'counter', DATA_TYPES.VARCHAR(), false, true);
        expect(createIndexStatus).eql(true);

        await populateTestTable(100);
        let results = await query(tableName, "$.counter  = 10", ['counter']);
        expect(results.length).eql(1);

        let deletedDocCount = await deleteDocuments(tableName, "$.counter  = 10", ['counter']);
        expect(deletedDocCount).eql(1);
        results = await query(tableName, "$.counter  = 10", ['counter']);
        expect(results.length).eql(0);

        // deleting again should delete no documents as there are none
        deletedDocCount = await deleteDocuments(tableName, "$.counter  = 10", ['counter']);
        expect(deletedDocCount).eql(0);
    });
    it('deleteDocuments with index field should delete multiple document', async function () {
        const createIndexStatus = await createIndexForJsonField(tableName, 'counter', DATA_TYPES.VARCHAR(), false, true);
        expect(createIndexStatus).eql(true);
        await populateTestTable(100);
        let results = await query(tableName, "$.counter  >= 10 AND $.counter  <= 27", ['counter']);
        expect(results.length).eql(18);

        let deletedDocCount = await deleteDocuments(tableName, "$.counter  >= 10 AND $.counter  <= 27", ['counter']);
        expect(deletedDocCount).eql(18);
        results = await query(tableName, "$.counter  >= 10 AND $.counter  <= 27", ['counter']);
        expect(results.length).eql(0);

        // deleting again should delete no documents as there are none
        deletedDocCount = await deleteDocuments(tableName, "$.counter  >= 10 AND $.counter  <= 27", ['counter']);
        expect(deletedDocCount).eql(0);

        // count remaining documents to verify nothing else gets deleted
        results = await query(tableName, "$.Age  = 100"); // all has age 100
        expect(results.length).eql(82);
    });

    // listDatabases, listTables, getTableIndexes integration tests
    it('listDatabases should return list including the test database', async function () {
        const databases = await listDatabases();
        expect(databases).to.be.an('array');
        expect(databases.length).to.be.greaterThan(0);
        expect(databases).to.include(database);
    });

    it('listTables should return list including the test table', async function () {
        const tables = await listTables(database);
        expect(tables).to.be.an('array');
        expect(tables).to.include('customers');
    });

    it('listTables should return empty array for database with no tables', async function () {
        const emptyDbName = 'emptydb';
        await createDataBase(emptyDbName);
        try {
            const tables = await listTables(emptyDbName);
            expect(tables).to.be.an('array');
            expect(tables.length).to.eql(0);
        } finally {
            await deleteDataBase(emptyDbName);
        }
    });

    it('listTables should fail for non-existent database', async function () {
        let isExceptionOccurred = false;
        try {
            await listTables('nonexistentdb12345');
        } catch (e) {
            isExceptionOccurred = true;
            expect(e.code).to.eql('ER_BAD_DB_ERROR');
        }
        expect(isExceptionOccurred).to.eql(true);
    });

    it('getTableIndexes should return PRIMARY key for table', async function () {
        const indexes = await getTableIndexes(tableName);
        expect(indexes).to.be.an('array');
        expect(indexes.length).to.be.greaterThan(0);

        // Find the PRIMARY index
        const primaryIndex = indexes.find(idx => idx.indexName === 'PRIMARY');
        expect(primaryIndex).to.exist;
        expect(primaryIndex.columnName).to.eql('documentID');
        expect(primaryIndex.isUnique).to.eql(true);
        expect(primaryIndex.isPrimary).to.eql(true);
        expect(primaryIndex.jsonField).to.eql(null);
    });

    it('getTableIndexes should return JSON field mapping for indexed fields', async function () {
        // Create an index on a JSON field
        const isSuccess = await createIndexForJsonField(tableName, 'lastName', DATA_TYPES.VARCHAR(50), false);
        expect(isSuccess).to.eql(true);

        const indexes = await getTableIndexes(tableName);
        expect(indexes).to.be.an('array');
        expect(indexes.length).to.be.greaterThan(1);

        // Find the index on lastName
        const lastNameIndex = indexes.find(idx => idx.jsonField === 'lastName');
        expect(lastNameIndex).to.exist;
        expect(lastNameIndex.isUnique).to.eql(false);
        expect(lastNameIndex.isPrimary).to.eql(false);
        expect(lastNameIndex.indexType).to.eql('BTREE');
    });

    it('getTableIndexes should return nested JSON field mapping', async function () {
        // Create an index on a nested JSON field
        const isSuccess = await createIndexForJsonField(tableName, 'location.city', DATA_TYPES.VARCHAR(100), false);
        expect(isSuccess).to.eql(true);

        const indexes = await getTableIndexes(tableName);
        expect(indexes).to.be.an('array');

        // Find the index on location.city
        const cityIndex = indexes.find(idx => idx.jsonField === 'location.city');
        expect(cityIndex).to.exist;
        expect(cityIndex.isUnique).to.eql(false);
        expect(cityIndex.isPrimary).to.eql(false);
    });

    it('getTableIndexes should fail for non-existent table', async function () {
        let isExceptionOccurred = false;
        try {
            await getTableIndexes(database + '.nonexistenttable12345');
        } catch (e) {
            isExceptionOccurred = true;
            expect(e.code).to.eql('ER_NO_SUCH_TABLE');
        }
        expect(isExceptionOccurred).to.eql(true);
    });

    // === listDatabases extended tests ===
    it('listDatabases should return system databases', async function () {
        const databases = await listDatabases();
        expect(databases).to.include('information_schema');
        expect(databases).to.include('mysql');
    });

    it('listDatabases should return multiple user databases', async function () {
        const db1 = 'listdb1';
        const db2 = 'listdb2';
        await createDataBase(db1);
        await createDataBase(db2);
        try {
            const databases = await listDatabases();
            expect(databases).to.include(db1);
            expect(databases).to.include(db2);
        } finally {
            await deleteDataBase(db1);
            await deleteDataBase(db2);
        }
    });

    // === listTables extended tests ===
    it('listTables should return tables with underscores and numbers', async function () {
        const specialTableName = database + '.test_table_123';
        await createTable(specialTableName);
        try {
            const tables = await listTables(database);
            expect(tables).to.include('test_table_123');
        } finally {
            await deleteTable(specialTableName);
        }
    });

    it('listTables should return multiple tables', async function () {
        const table1 = database + '.listtable1';
        const table2 = database + '.listtable2';
        const table3 = database + '.listtable3';
        await createTable(table1);
        await createTable(table2);
        await createTable(table3);
        try {
            const tables = await listTables(database);
            expect(tables).to.include('listtable1');
            expect(tables).to.include('listtable2');
            expect(tables).to.include('listtable3');
        } finally {
            await deleteTable(table1);
            await deleteTable(table2);
            await deleteTable(table3);
        }
    });

    it('listTables should return empty array after all tables deleted', async function () {
        const tempDb = 'tempdbfortabletest';
        const tempTable = tempDb + '.temptable';
        await createDataBase(tempDb);
        try {
            await createTable(tempTable);
            let tables = await listTables(tempDb);
            expect(tables).to.include('temptable');

            await deleteTable(tempTable);
            tables = await listTables(tempDb);
            expect(tables).to.be.an('array');
            expect(tables.length).to.eql(0);
        } finally {
            await deleteDataBase(tempDb);
        }
    });

    // === getTableIndexes extended tests ===
    it('getTableIndexes should return unique index with isUnique true', async function () {
        const isSuccess = await createIndexForJsonField(tableName, 'email', DATA_TYPES.VARCHAR(100), true);
        expect(isSuccess).to.eql(true);

        const indexes = await getTableIndexes(tableName);
        const emailIndex = indexes.find(idx => idx.jsonField === 'email');
        expect(emailIndex).to.exist;
        expect(emailIndex.isUnique).to.eql(true);
    });

    it('getTableIndexes should return isNullable true for nullable columns', async function () {
        // Create nullable index (default - isNotNull = false)
        const isSuccess = await createIndexForJsonField(tableName, 'optionalField', DATA_TYPES.VARCHAR(50), false);
        expect(isSuccess).to.eql(true);

        const indexes = await getTableIndexes(tableName);
        const optionalIndex = indexes.find(idx => idx.jsonField === 'optionalField');
        expect(optionalIndex).to.exist;
        expect(optionalIndex.isNullable).to.eql(true);
    });

    it('getTableIndexes should return isNullable false for NOT NULL columns', async function () {
        // Create non-null index
        const isSuccess = await createIndexForJsonField(tableName, 'requiredField', DATA_TYPES.VARCHAR(50), false, true);
        expect(isSuccess).to.eql(true);

        const indexes = await getTableIndexes(tableName);
        const requiredIndex = indexes.find(idx => idx.jsonField === 'requiredField');
        expect(requiredIndex).to.exist;
        expect(requiredIndex.isNullable).to.eql(false);
    });

    it('getTableIndexes should handle deep nested JSON fields (3+ levels)', async function () {
        const isSuccess = await createIndexForJsonField(tableName, 'location.address.street.name', DATA_TYPES.VARCHAR(100), false);
        expect(isSuccess).to.eql(true);

        const indexes = await getTableIndexes(tableName);
        const deepIndex = indexes.find(idx => idx.jsonField === 'location.address.street.name');
        expect(deepIndex).to.exist;
        expect(deepIndex.isPrimary).to.eql(false);
    });

    it('getTableIndexes should handle field names with underscores and numbers', async function () {
        const isSuccess = await createIndexForJsonField(tableName, 'user_id_123', DATA_TYPES.INT, false);
        expect(isSuccess).to.eql(true);

        const indexes = await getTableIndexes(tableName);
        const fieldIndex = indexes.find(idx => idx.jsonField === 'user_id_123');
        expect(fieldIndex).to.exist;
    });

    it('getTableIndexes should return multiple indexes with correct properties', async function () {
        // Create multiple indexes
        await createIndexForJsonField(tableName, 'firstName', DATA_TYPES.VARCHAR(50), false);
        await createIndexForJsonField(tableName, 'uniqueId', DATA_TYPES.VARCHAR(50), true);
        await createIndexForJsonField(tableName, 'count', DATA_TYPES.INT, false);

        const indexes = await getTableIndexes(tableName);

        // Verify PRIMARY key
        const primary = indexes.find(idx => idx.isPrimary);
        expect(primary).to.exist;
        expect(primary.isUnique).to.eql(true);

        // Verify non-unique index
        const firstNameIdx = indexes.find(idx => idx.jsonField === 'firstName');
        expect(firstNameIdx).to.exist;
        expect(firstNameIdx.isUnique).to.eql(false);

        // Verify unique index
        const uniqueIdIdx = indexes.find(idx => idx.jsonField === 'uniqueId');
        expect(uniqueIdIdx).to.exist;
        expect(uniqueIdIdx.isUnique).to.eql(true);

        // Verify all have correct indexType
        indexes.forEach(idx => {
            expect(idx.indexType).to.eql('BTREE');
        });
    });

    it('getTableIndexes should return jsonField null for non-generated columns', async function () {
        const indexes = await getTableIndexes(tableName);

        // PRIMARY key on documentID is not a generated column
        const primary = indexes.find(idx => idx.isPrimary);
        expect(primary).to.exist;
        expect(primary.jsonField).to.eql(null);
        expect(primary.columnName).to.eql('documentID');
    });

    it('getTableIndexes should verify sequenceInIndex for all indexes', async function () {
        await createIndexForJsonField(tableName, 'seqTest', DATA_TYPES.VARCHAR(50), false);

        const indexes = await getTableIndexes(tableName);

        // All single-column indexes should have sequenceInIndex = 1
        indexes.forEach(idx => {
            expect(idx.sequenceInIndex).to.eql(1);
        });
    });

    it('getTableIndexes should handle both unique and non-unique indexes on same table', async function () {
        // Create a unique index
        await createIndexForJsonField(tableName, 'uniqueEmail', DATA_TYPES.VARCHAR(100), true, true);
        // Create a non-unique index
        await createIndexForJsonField(tableName, 'category', DATA_TYPES.VARCHAR(50), false);

        const indexes = await getTableIndexes(tableName);

        const uniqueEmailIdx = indexes.find(idx => idx.jsonField === 'uniqueEmail');
        expect(uniqueEmailIdx).to.exist;
        expect(uniqueEmailIdx.isUnique).to.eql(true);
        expect(uniqueEmailIdx.isNullable).to.eql(false);

        const categoryIdx = indexes.find(idx => idx.jsonField === 'category');
        expect(categoryIdx).to.exist;
        expect(categoryIdx.isUnique).to.eql(false);
        expect(categoryIdx.isNullable).to.eql(true);
    });
});

async function testReadWrite(numberOfWrites) {
    const {document, documentIds, tableName} = await populateTestTable(numberOfWrites);
    const readPromises = [];
    documentIds.forEach(id => {
        let readPromise = get(tableName, id);
        readPromises.push(readPromise);

    });
    const getReturns = await Promise.all(readPromises);
    expect(getReturns.length).to.eql(numberOfWrites);
    getReturns.forEach(results => {
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);
    });
    return {
        'tableName': tableName,
        'document': document,
        'documentIds': documentIds
    };
}

async function populateTestTable(numberOfWrites) {
    const document = {
        'lastName': 'Alice',
        'userId': "ABCD",
        'Age': 100,
        'active': true,
        'location': {
            'city': 'Banglore',
            'state': 'Karnataka',
            'layout': {
                'block': '1stblock'
            }
        }
    };
    const writePromises = [];
    for (let i = 0; i < numberOfWrites; i++) {
        document.counter = i;
        let retPromise = put(tableName, structuredClone(document));
        writePromises.push(retPromise);
    }
    const documentIds = await Promise.all(writePromises);
    expect(documentIds.length).to.eql(numberOfWrites);
    documentIds.forEach(id => {
        expect(id.length).to.eql(32);
    });
    return {
        'tableName': tableName,
        'document': document,
        'documentIds': documentIds
    };
}
