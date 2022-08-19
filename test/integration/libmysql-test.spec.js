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
import {
    createIndexForJsonField,
    createTable,
    deleteKey,
    deleteTable, getFromIndex,
    get,
    getFromNonIndex,
    put
} from "../../src/index.js";
import {init, close} from "../../src/utils/db.js";
import {isObjectEmpty} from "@aicore/libcommonutils";
import * as crypto from "crypto";

let expect = chai.expect;

const tableName = 'customer';
describe('Integration: libMySql', function () {
    after(function () {
        close();
    });
    before(async function () {
        const configs = await getMySqlConfigs();
        console.log(`${JSON.stringify(configs)}`);
        init(configs);


    });

    beforeEach(async function () {

        await createTable(tableName);
    });
    afterEach(async function () {
        await deleteTable(tableName);
    });

    it('should create table add data and get data', async function () {
        const valueOfJson = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        const docId = await put(tableName, valueOfJson);
        const results = await get(tableName, docId);
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);
        const resultNonIndex = await getFromNonIndex(tableName, {
            'lastName': 'Alice',
            'Age': 100
        });
        // delete key
        await deleteKey(tableName, docId);
        const deletedValue = await get(tableName, docId);
        expect(isObjectEmpty(deletedValue)).to.eql(true);


    });

    it('get should return empty if data not present', async function () {

        const docId = crypto.randomBytes(64).toString('hex');
        const getReturn = await get(tableName, docId);
        expect(isObjectEmpty(getReturn)).to.eql(true);
    });
    it('get should throw exception table is not present', async function () {
        let exceptionOccurred = false;
        try {
            const docId = crypto.randomBytes(64).toString('hex');
            await get('HELLO', docId);

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
            await put('hello', document);
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

// ToDo add update API
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
        const docId2 = await put(tableName, document);
        results = await get(tableName, docId2);
        expect(results.lastName).to.eql(document.lastName);
        expect(results.Age).to.eql(document.Age);
        expect(results.active).to.eql(document.active);

        await deleteKey(tableName, docId1);
        await deleteKey(tableName, docId2);

    });
    it('should be able to do scan and return results from database', async function () {
        const numberOfEntries = 100;
        const results = await testReadWrite(numberOfEntries);
        const scanResults = await getFromNonIndex(results.tableName, {
            'lastName': 'Alice',
            'Age': 100
        });
        expect(scanResults.length).to.eql(numberOfEntries);
        for (let i = 0; i < scanResults.length; i++) {
            expect(scanResults[i].Age).to.eql(100);
            expect(scanResults[i].active).to.eql(true);
            expect(scanResults[i].lastName).to.eql('Alice');
        }
        await deleteData(results);
    });

    it('delete table should pass if table does not exit', async function () {
        const isSuccess = await deleteTable('hello');
        expect(isSuccess).to.eql(true);

    });
    it('create and validate Index should pass', async function () {
        const numberOfEntries = 1000;
        const results = await testReadWrite(numberOfEntries);
        let isSuccess = await createIndexForJsonField(tableName, 'lastName', 'VARCHAR(50)', false);
        expect(isSuccess).to.eql(true);
        isSuccess = await createIndexForJsonField(tableName, 'Age', 'INT', false);
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
        });
        await deleteData(results);
    });
    it('create and validate Index return empty list if nothing matches', async function () {
        const numberOfEntries = 1000;
        let isSuccess = await createIndexForJsonField(tableName, 'lastName', 'VARCHAR(50)', false);
        expect(isSuccess).to.eql(true);
        isSuccess = await createIndexForJsonField(tableName, 'Age', 'INT', false);
        expect(isSuccess).to.eql(true);
        const queryResults = await getFromIndex(tableName, {
            'lastName': 'Alice',
            'Age': 100
        });
        expect(queryResults.length).to.eql(0);

    });
});

async function testReadWrite(numberOfWrites) {
    const tableName = 'customer';
    const document = {
        'lastName': 'Alice',
        'Age': 100,
        'active': true
    };
    const writePromises = [];
    for (let i = 0; i < numberOfWrites; i++) {
        let retPromise = put(tableName, document);
        writePromises.push(retPromise);
    }
    const documentIds = await Promise.all(writePromises);
    expect(documentIds.length).to.eql(numberOfWrites);
    documentIds.forEach(id => {
        expect(id.length).to.eql(128);
    });
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
