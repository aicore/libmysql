/*global describe, it*/
import mockedFunctions from '../setup-mocks.js';
import Query, {QueryTokenizer} from "../../../src/utils/query.js";
import chai from "chai";
let expect = chai.expect;

describe('Query Utils test', function () {
    describe('QueryTokenizer tests', function () {
        it('should throw error on unknown token', function () {
            let exceptionOccurred = false;
            let tokenizer = QueryTokenizer.getTokenizer("@oops");
            try {
                QueryTokenizer.nextToken(tokenizer);
            } catch (e) {
                exceptionOccurred = true;
                expect(e.toString()).to.eql('Error: Unexpected Token char @ in query @oops');
            }
            expect(exceptionOccurred).to.eql(true);
        });

        it('should tokenizer work in empty string', function () {
            let tokenizer = QueryTokenizer.getTokenizer("");
            let token = QueryTokenizer.nextToken(tokenizer);
            expect(token).to.be.null;
        });

        it('should tokenizer extract space token', function () {
            let tokenizer = QueryTokenizer.getTokenizer("   ");
            let token = QueryTokenizer.nextToken(tokenizer);
            expect(token.str).to.eql("   "); // contain 3 spaces
        });
    });

    describe('Query Transformer tests', function () {
        it('should AND index and non index queries', function () {
            expect(Query.transformCocoToSQLQuery("", "")).to.eql("");
            expect(Query.transformCocoToSQLQuery("x", "")).to.eql("x");
            expect(Query.transformCocoToSQLQuery("", "x")).to.eql("x");
            expect(Query.transformCocoToSQLQuery("x", "y")).to.eql("x AND y");
        });
    });
});
