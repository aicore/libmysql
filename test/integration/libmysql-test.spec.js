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
/*global describe, it, after, before*/

import * as chai from 'chai';
import {getMySqlConfigs} from './setupIntegTest.js';
import {createTable, get, put} from "../../src/index.js";
import {init, close} from "../../src/utils/db.js";
import {isObjectEmpty} from "@aicore/libcommonutils";
import * as crypto from "crypto";

let expect = chai.expect;

describe('Integration: libMySql', function () {
    after(function () {
        close();
    });
    before(async function () {
        const configs = await getMySqlConfigs();
        console.log(`${JSON.stringify(configs)}`);
        init(configs);

    });

    it('should create table add data and get data', async function () {
        const tableName = 'customer';
        const nameOfPrimaryKey = 'name';
        const nameOfJsonColumn = 'details';
        await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
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

    });

    it('get should return empty if data not present', async function () {
        const tableName = 'customer';
        const nameOfPrimaryKey = 'name';
        const nameOfJsonColumn = 'details';
        const primaryKey = 'raj';
        const getReturn = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        expect(isObjectEmpty(getReturn)).to.eql(true);
    });
    it('get should throw exception table is not present', async function () {
        let exceptionOccurred = false;
        try {
            const tableName = 'abc';
            const nameOfPrimaryKey = 'name';
            const nameOfJsonColumn = 'details';
            const primaryKey = 'raj';
            await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);

        } catch (e) {
            exceptionOccurred = true;
            expect(e.code).to.eql('ER_NO_SUCH_TABLE');
        }
        expect(exceptionOccurred).to.eql(true);
    });

    it('put should throw exception table is not present', async function () {
        let exceptionOccurred = false;
        try {
            const tableName = 'abc';
            const nameOfPrimaryKey = 'name';
            const nameOfJsonColumn = 'details';
            const primaryKey = 'bob';
            const valueOfJson = {
                'lastName': 'Alice',
                'Age': 100,
                'active': true
            };
            await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.code).to.eql('ER_NO_SUCH_TABLE');
        }
        expect(exceptionOccurred).to.eql(true);
    });
    it('100 writes followed by read', async function () {
        await testReadWrite(100);
    });

    it('1000 writes followed by read', async function () {
        await testReadWrite(1000);
    });
    it('1500 writes followed by read', async function () {
        await testReadWrite(1500);
    });

    it('should be able to update data', async function () {

        const tableName = 'customer';
        const nameOfPrimaryKey = 'name';
        const nameOfJsonColumn = 'details';
        const primaryKey = 'bob';
        let valueOfJson = {
            'lastName': 'Alice',
            'Age': 100,
            'active': true
        };
        await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
        let results1 = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        let results = JSON.parse(results1);
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);

        valueOfJson = {
            'lastName': 'Alice1',
            'Age': 140,
            'active': true
        };
        await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn, valueOfJson);
        let results2 = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColumn);
        results = JSON.parse(results2);
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);

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
    console.log((`${JSON.stringify(valueOfJson)}`));
    getReturns.forEach(results => {
        expect(results.lastName).to.eql(valueOfJson.lastName);
        expect(results.Age).to.eql(valueOfJson.Age);
        expect(results.active).to.eql(valueOfJson.active);
    });
}
