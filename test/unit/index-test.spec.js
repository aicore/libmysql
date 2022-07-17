/*global describe, it, before*/
import * as chai from 'chai';
import {createTable} from "../../src/index.js";
import {init} from "../../src/utils/db.js";
import {getMySqlConfigs} from "@aicore/libcommonutils";

let expect = chai.expect;
describe('testing src/index.js', function () {
    before(function () {
        init(getMySqlConfigs());

    });

    it('createTable should pass', async function () {
        const result = await createTable('customers', 'id', 'customer_data');
        expect(result.results.ResultSetHeader.serverStatus).to.eql(2);
    });
});
