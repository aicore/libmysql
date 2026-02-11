import mockedFunctions from '../setup-mocks.js';
import * as chai from 'chai';
import {
    init,
    close,
    getFromNonIndex
} from "../../../src/utils/db.js";
import {getMySqlConfigs} from "@aicore/libcommonutils";

let expect = chai.expect;

describe('Unit tests for db.js', function () {
    beforeEach(function () {
        close();
        init(getMySqlConfigs());
    });

    it('getFromNonIndex key sql injection attack test', async function () {
        const saveExecute = mockedFunctions.connection.query;
        mockedFunctions.connection.query = function (sql, values, callback) {
            callback(null, [], []);
        };
        const tableName = 'test.hello';
        let isExceptionOccurred = false;
        try {
            await getFromNonIndex(tableName, {
                "yo\"= \"x\" OR 1 != (SELECT COUNT(*) FROM test.arun) OR not \"1": "x"
            });
        } catch (e) {
            expect(e.split('\n')[0]).to.eql(
                `Invalid filed name yo"= "x" OR 1 != (SELECT COUNT(*) FROM test.arun) OR not "1`);
            isExceptionOccurred = true;
        }
        expect(isExceptionOccurred).to.eql(true);
        mockedFunctions.connection.query = saveExecute;
    });
});
