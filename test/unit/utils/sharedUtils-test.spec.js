/*global describe, it*/
import mockedFunctions from '../setup-mocks.js';
import {isAlphaNumChar, isAlphaChar, isVariableNameLike} from "../../../src/utils/sharedUtils.js";
import chai from "chai";
let expect = chai.expect;

describe('Shared Utils test', function () {
    it('should isAlphaChar work', function () {
        expect(isAlphaChar('a')).to.be.true;
        expect(isAlphaChar('A')).to.be.true;

        expect(isAlphaChar('0')).to.be.false;
        expect(isAlphaChar('_')).to.be.false;
    });

    it('should isAlphaNumChar work', function () {
        expect(isAlphaNumChar('a')).to.be.true;
        expect(isAlphaNumChar('A')).to.be.true;
        expect(isAlphaNumChar('0')).to.be.true;
        expect(isAlphaNumChar('9')).to.be.true;

        expect(isAlphaNumChar('_')).to.be.false;
    });

    it('should isVariableNameLike work', function () {
        expect(isVariableNameLike('aA8_')).to.be.true;
        expect(isVariableNameLike('_')).to.be.true;
        expect(isVariableNameLike('HELLO_WORLD_9_')).to.be.true;
        expect(isVariableNameLike('camelCase8')).to.be.true;

        expect(isVariableNameLike('8')).to.be.false;
        expect(isVariableNameLike('8asc')).to.be.false;
        expect(isVariableNameLike('8asc#')).to.be.false;
        expect(isVariableNameLike('#')).to.be.false;
        expect(isVariableNameLike('.ads')).to.be.false;
        expect(isVariableNameLike('s.ads')).to.be.false;
    });
});
