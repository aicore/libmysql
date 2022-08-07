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
import {createTable, deleteKey, deleteTable, get, getFromNonIndex, put} from "../../src/index.js";
import {init, close} from "../../src/utils/db.js";
import {isObjectEmpty} from "@aicore/libcommonutils";
import * as crypto from "crypto";

let expect = chai.expect;

const tableName = 'customer';
const nameOfPrimaryKey = 'name';
const nameOfJsonColumn = 'details';

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

        await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
    });
    afterEach(async function () {
        await deleteTable(tableName);
    });

    it('should create table add data and get data', async function () {
        const primaryKey = 'bob';
        const valueOfJson = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
        const results = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);
        const resultNonIndex = await getFromNonIndex(tableName, nameOfJsonColumn, {
            'lastName': 'Alice',
            'Age': 100
        });
        // delete key
        await deleteKey(tableName, nameOfPrimaryKey, primaryKey);
        const deletedValue = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        expect(isObjectEmpty(deletedValue)).to.eql(true);


    });

    it('get should return empty if data not present', async function () {

        const primaryKey = 'raj';
        const getReturn = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        expect(isObjectEmpty(getReturn)).to.eql(true);
    });
    it('get should throw exception table is not present', async function () {
        let exceptionOccurred = false;
        try {

            const primaryKey = 'raj';
            await get('HELLO', nameOfPrimaryKey, primaryKey, nameOfJsonColumn);

        } catch (e) {
            exceptionOccurred = true;
            expect(e.code).to.eql('ER_NO_SUCH_TABLE');
        }
        expect(exceptionOccurred).to.eql(true);
    });

    it('put should throw exception table is not present', async function () {
        let exceptionOccurred = false;
        try {

            const primaryKey = 'bob';
            const valueOfJson = {
                'lastName': 'Alice',
                'Age': 100,
                'active': true
            };
            await put('hello', nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
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
        for (let i = 0; i < results.primaryKeys.length; i++) {
            let deletePromise = deleteKey(results.tableName, results.nameOfPrimaryKey, results.primaryKeys[i]);
            deletePromises.push(deletePromise);
        }
        const deleteReturns = await Promise.all(deletePromises);
        expect(deleteReturns.length).to.eql(deletePromises.length);
        for (let i = 0; i < results.primaryKeys.length; i++) {
            expect(deleteReturns[i]).to.eql(true);
        }
    }

    it('should be able to update data', async function () {

        const primaryKey = 'bob';
        let valueOfJson = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
        let results = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);

        valueOfJson = {
            'lastName': 'Alice1',
            'Age': 140,
            'active': true
        };
        await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
        results = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);

        await deleteKey(tableName, nameOfPrimaryKey, primaryKey);

    });
    it('should be able to do scan and return results from database', async function () {
        const numberOfEntries = 100;
        const results = await testReadWrite(numberOfEntries);
        const scanResults = await getFromNonIndex(results.tableName, results.nameOfJsonColumn, {
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
});

async function testReadWrite(numberOfWrites) {
    const tableName = 'customer';
    const nameOfPrimaryKey = 'name';
    const nameOfJsonColumn = 'details';
    // const primaryKey = 'bob';
    const valueOfJson = {
        'lastName': 'Alice',
        'Age': 100,
        'active': true
    };
    const writePromises = [];
    const primaryKeys = [];
    for (let i = 0; i < numberOfWrites; i++) {
        let primaryKey = crypto.randomBytes(4).toString('hex');
        let retPromise = put(tableName, nameOfPrimaryKey,
            primaryKey, nameOfJsonColumn, valueOfJson);
        writePromises.push(retPromise);
        primaryKeys.push(primaryKey);
    }
    const putReturns = await Promise.all(writePromises);
    expect(putReturns.length).to.eql(numberOfWrites);
    putReturns.forEach(returns => {
        expect(returns).to.eql(true);
    });
    const readPromises = [];
    for (let i = 0; i < numberOfWrites; i++) {
        let readPromise = get(tableName, nameOfPrimaryKey, primaryKeys[i], nameOfJsonColumn);
        readPromises.push(readPromise);
    }
    const getReturns = await Promise.all(readPromises);
    expect(getReturns.length).to.eql(numberOfWrites);
    getReturns.forEach(results => {
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);
    });
    return {
        'tableName': tableName,
        'nameOfPrimaryKey': nameOfPrimaryKey,
        'nameOfJsonColumn': nameOfJsonColumn,
        'valueOfJson': valueOfJson,
        'primaryKeys': primaryKeys
    };
}
