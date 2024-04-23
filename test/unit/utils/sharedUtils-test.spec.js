/*global describe, it*/
import mockedFunctions from '../setup-mocks.js';
import {
    isAlphaNumChar, isAlphaChar, isVariableNameLike,
    isNestedVariableNameLike, isDigitChar
} from "../../../src/utils/sharedUtils.js";
import * as chai from 'chai';
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

    it('should isDigitChar work', function () {
        expect(isDigitChar('0')).to.be.true;
        expect(isDigitChar('9')).to.be.true;

        expect(isDigitChar('_')).to.be.false;
        expect(isDigitChar('a')).to.be.false;
        expect(isDigitChar('A')).to.be.false;

    });

    function _validateVarNames(validatorFn) {
        expect(validatorFn('aA8_')).to.be.true;
        expect(validatorFn('_')).to.be.true;
        expect(validatorFn('HELLO_WORLD_9_')).to.be.true;
        expect(validatorFn('camelCase8')).to.be.true;

        expect(validatorFn('8')).to.be.false;
        expect(validatorFn('8asc')).to.be.false;
        expect(validatorFn('8asc#')).to.be.false;
        expect(validatorFn('#')).to.be.false;
        expect(validatorFn('.ads')).to.be.false;
    }

    it('should isVariableNameLike work', function () {
        _validateVarNames(isVariableNameLike);
        expect(isVariableNameLike('s.ads')).to.be.false;
    });

    it('should isNestedVariableNameLike work', function () {
        _validateVarNames(isNestedVariableNameLike);
        expect(isNestedVariableNameLike('aA8_.aA8_')).to.be.true;
        expect(isNestedVariableNameLike('_._')).to.be.true;
        expect(isNestedVariableNameLike('HELLO_.WORLD._9._')).to.be.true;
        expect(isNestedVariableNameLike('camelCase8.camelCase8')).to.be.true;

        expect(isNestedVariableNameLike('8')).to.be.false;
        expect(isNestedVariableNameLike('8asc')).to.be.false;
        expect(isNestedVariableNameLike('8asc#')).to.be.false;
        expect(isNestedVariableNameLike('#')).to.be.false;
        expect(isNestedVariableNameLike('.ads')).to.be.false;
        expect(isNestedVariableNameLike('ads.')).to.be.false;
        expect(isNestedVariableNameLike('HELLO_.WORLD. _9._')).to.be.false;
        expect(isNestedVariableNameLike('HELLO_.WORLD.._')).to.be.false;
    });
});
