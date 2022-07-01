/*global describe, it*/

import * as assert from 'assert';
import * as chai from 'chai';

let expect = chai.expect;
import {getMySqlConfigs} from "../../../src/utils/configs.js";

describe('unit test for getMySqlConfigs', function () {
    it('validate getMysqlConfigsDefault', function () {
        const configs = getMySqlConfigs();
        const host = configs.host;
        const port = configs.port;
        expect(port).to.eql('3306');
        expect(host).to.eql('localhost');
    });
});
