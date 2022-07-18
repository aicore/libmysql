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
/*global describe, it, after*/

import * as assert from 'assert';
import * as chai from 'chai';
import {getMySqlConfigs} from './setupIntegTest.js';
import {createTable, get, put} from "../../src/index.js";
import {init, close} from "../../src/utils/db.js";


let expect = chai.expect;

describe('Integration: libMySql', function () {

    after(function () {
        close();
    });

    it('should create table', async function () {
        try {
            const configs = await getMySqlConfigs();
            console.log(`${JSON.stringify(configs)}`);
            init(configs);
            const tableName = 'customer';
            const nameOfPrimaryKey = 'name';
            const nameOfJsonColoumn = 'details';

            const result = await createTable(tableName, nameOfPrimaryKey, nameOfJsonColoumn);
            console.log(`createTable ${JSON.stringify(result)}`);
            const primaryKey = 'bob';
            const valueOfJsonColoumn = {
                'lastName': 'Alics',
                'Age': 100,
                'active': true
            };
            const putReturn = await put(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColoumn, JSON.stringify(valueOfJsonColoumn));
            console.log(`Put return ${JSON.stringify(putReturn)}`);
            const getReturn = await get(tableName, nameOfPrimaryKey, primaryKey, nameOfJsonColoumn);
            const results = getReturn.results[0].details;
            expect(results.lastName).to.eql(valueOfJsonColoumn.lastName);
            expect(results.Age).to.eql(valueOfJsonColoumn.Age);
            expect(results.active).to.eql(valueOfJsonColoumn.active);

            console.log(`get return ${JSON.stringify(getReturn)}`);
        } catch (e) {
            console.log(`printing stack trace ${JSON.stringify(e)}`);
        }

    });
});
