/*global describe, it*/
import * as chai from 'chai';
import {createTable} from "../../src/index.js";

let expect = chai.expect;
describe('testing src/index.js', function () {
    it('createTable should pass', async function () {
        const result = await createTable('customers', 'id', 'customer_data');
        expect(result.results.ResultSetHeader.serverStatus).to.eql(2);
    });
});
