/*global describe, it, before, beforeEach*/
import mockedFunctions from '../setup-mocks.js';
import {close, init} from "../../../src/utils/db.js";
import {getMySqlConfigs} from "@aicore/libcommonutils";
import chai from "chai";
let expect = chai.expect;
describe('init test', function () {
    before(function () {
        close();
    });

    it('should fail if connection if config is invalid', function () {
        let exceptionOccurred = false;
        try {
            init(null);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please provide valid config');
        }
        expect(exceptionOccurred).to.eql(true);

    });
    it('should fail if connection if host is invalid', function () {
        let exceptionOccurred = false;
        try {
            const config = getMySqlConfigs();
            config.host = null;
            init(config);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please provide valid hostname');
        }
        expect(exceptionOccurred).to.eql(true);
    });
    it('should fail if connection if port is invalid', function () {
        let exceptionOccurred = false;
        try {
            const config = getMySqlConfigs();
            config.port = null;
            init(config);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please provide valid port');
        }
        expect(exceptionOccurred).to.eql(true);
    });
    it('should fail if connection if user is invalid', function () {
        let exceptionOccurred = false;
        try {
            const config = getMySqlConfigs();
            config.user = null;
            init(config);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please provide valid user');
        }
        expect(exceptionOccurred).to.eql(true);
    });
    it('should fail if connection if user is password', function () {
        let exceptionOccurred = false;
        try {
            const config = getMySqlConfigs();
            config.password = null;
            init(config);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please provide valid password');
        }
        expect(exceptionOccurred).to.eql(true);
    });

    it('should fail if connection if user is database', function () {
        let exceptionOccurred = false;
        try {
            const config = getMySqlConfigs();
            config.database = null;
            init(config);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: Please provide valid database');
        }
        expect(exceptionOccurred).to.eql(true);
    });

    it('init should pass', function () {
        let exceptionOccurred = false;
        try {
            const config = getMySqlConfigs();
            init(config);
        } catch (e) {
            exceptionOccurred = true;
        }
        expect(exceptionOccurred).to.eql(false);
    });
    it('init should fail if two init is called', function () {
        let exceptionOccurred = false;
        try {
            const config = getMySqlConfigs();
            init(config);
            init(config);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql('Error: One connection is active please close it before reinitializing it');
        }
        expect(exceptionOccurred).to.eql(true);
    });
});
