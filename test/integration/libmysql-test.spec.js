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

import {init, close, createDataBase, deleteDataBase, mathAdd, query, deleteDocuments} from "../../src/utils/db.js";
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
