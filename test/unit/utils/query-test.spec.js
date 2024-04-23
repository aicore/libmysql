/*global describe, it*/
import mockedFunctions from '../setup-mocks.js';
import Query, {QueryTokenizer} from "../../../src/utils/query.js";
import * as chai from 'chai';
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

        function _verifyToken(token, expectedType, expectedTokenString) {
            expect(token.type).to.eql(expectedType); // contain 3 spaces
            expect(token.str).to.eql(expectedTokenString); // contain 3 spaces
        }

        it('should tokenizer extract space token', function () {
            let tokenizer = QueryTokenizer.getTokenizer("   ");
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, " ", "   "); // contain 3 spaces
        });

        it('should tokenizer extract brackets (, ) and $ token', function () {
            let tokenizer = QueryTokenizer.getTokenizer("(  ()$"); // tokenizer does not check syntax validity
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "(", "(");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, " ", "  ");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "(", "(");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, ")", ")");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "#", "$");
            token = QueryTokenizer.nextToken(tokenizer);
            expect(token).to.be.null;
        });

        // string token tests
        it('should throw error on double quoted strings token', function () {
            let exceptionOccurred = false;
            let tokenizer = QueryTokenizer.getTokenizer('("hello"');
            try {
                QueryTokenizer.nextToken(tokenizer); // (
                QueryTokenizer.nextToken(tokenizer); // " // throws
            } catch (e) {
                exceptionOccurred = true;
                expect(e.toString()).to.eql(`Error: Strings Should Be in single quotes(Eg 'str') in query ("hello"`);
            }
            expect(exceptionOccurred).to.eql(true);
        });

        it('should tokenizer extract single quote string token', function () {
            let tokenizer = QueryTokenizer.getTokenizer("   'hello'   ");
            QueryTokenizer.nextToken(tokenizer); // skip the space
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "'", "'hello'");
        });

        it('should tokenizer extract single quote string token with no ending quotes', function () {
            let tokenizer = QueryTokenizer.getTokenizer("   'hello  ");
            QueryTokenizer.nextToken(tokenizer); // skip the space
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "'", "'hello  ");
        });

        it('should tokenizer extract single quote string token with special or other token chars', function () {
            let tokenizer = QueryTokenizer.getTokenizer("   'hello #$% (234) a.b'  ");
            QueryTokenizer.nextToken(tokenizer); // skip the space
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "'", "'hello #$% (234) a.b'");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, " ", "  "); // last two space
        });

        it('should tokenizer extract single quote string token with escape chars', function () {
            let tokenizer = QueryTokenizer.getTokenizer("('hello \\'a.b\\' yo')");
            QueryTokenizer.nextToken(tokenizer); // skip the (
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "'", "'hello \\'a.b\\' yo'");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, ")", ")"); // last bracket
        });

        // variables
        it('should tokenizer extract variable token of form $.xxx', function () {
            let tokenizer = QueryTokenizer.getTokenizer("   $.var($.var_)$._var");
            QueryTokenizer.nextToken(tokenizer); // skip the space
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "#", "$.var");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "(", "(");
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "#", "$.var_");
            QueryTokenizer.nextToken(tokenizer);
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "#", "$._var");
        });

        it('should tokenizer extract variable token of form $.xxx.yyy.zzz', function () {
            let tokenizer = QueryTokenizer.getTokenizer(" $.xxx($.xxx.yyy)$.xxx.yy_y._zzz");
            QueryTokenizer.nextToken(tokenizer); // skip the space
            let token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "#", "$.xxx");
            QueryTokenizer.nextToken(tokenizer); // skip the space
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "#", "$.xxx.yyy");
            QueryTokenizer.nextToken(tokenizer); // skip the space
            token = QueryTokenizer.nextToken(tokenizer);
            _verifyToken(token, "#", "$.xxx.yy_y._zzz");
        });

        function _verifyAllTokens(expressionString, expectedTypeArray, expectedTokenStringArray) {
            let tokenizer = QueryTokenizer.getTokenizer(expressionString);
            let token = QueryTokenizer.nextToken(tokenizer),
                str = [], type = [];
            while(token){
                str.push(token.str);
                type.push(token.type);
                token = QueryTokenizer.nextToken(tokenizer);
            }

            expect(type).to.eql(expectedTypeArray);
            expect(str).to.eql(expectedTokenStringArray);
        }

        function _verifyTokenParseError(expressionString, expectedErrorStr) {
            let exceptionOccurred = false;
            try {
                let tokenizer = QueryTokenizer.getTokenizer(expressionString);
                let token = QueryTokenizer.nextToken(tokenizer);
                while(token){
                    token = QueryTokenizer.nextToken(tokenizer);
                }
            } catch (e) {
                exceptionOccurred = true;
                expect(e.toString()).to.eql(expectedErrorStr);
            }
            expect(exceptionOccurred).to.eql(true);
        }

        //numbers
        it('should tokenizer extract number tokens of form 0, .0 and 0.0', function () {
            _verifyAllTokens("0", ["1"], ["0"]);
            _verifyAllTokens("0 (0.05).0 .123 45.234",
                ["1", " ", "(", "1", ")", "1", " ", "1", " ", "1"],
                ["0", " ", "(", "0.05", ")", ".0", " ", ".123", " ", "45.234"]);
        });

        it('should tokenizer error on invalid number tokens', function () {
            _verifyTokenParseError("0.0.0", "Error: Unexpected Number Token 0.0.0 in query 0.0.0");
            _verifyTokenParseError("0.x.0", "Error: Unexpected Number Token 0. in query 0.x.0");
            _verifyTokenParseError("0.%", "Error: Unexpected Number Token 0. in query 0.%");
        });

        //operators
        it('should tokenizer extract operator tokens', function () {
            _verifyAllTokens("!0 + 1 = (3.0 - 'hello') /.3%4*5 !=  7",
                ["!", "1", " ", "+", " ", "1", " ", "=", " ", "(", "1", " ", "-", " ", "'", ")", " ",  "/",
                    "1", "%", "1", "*", "1",  " ",  "!=", " ", "1"],
                ["!", "0", " ", "+", " ", "1", " ", "=", " ", "(", "3.0", " ", "-", " ", "'hello'", ")", " ",
                    "/", ".3", "%", "4", "*", "5",  " ",  "!=", "  ", "7"]);
        });

        it('should tokenizer extract comparison operator tokens', function () {
            _verifyAllTokens("1 > 2 1>=2 1<2 1<=2 1=2",
                ["1", " ", ">", " ", "1", " ", "1", ">=", "1", " ", "1", "<", "1", " ", "1", "<=", "1",
                    " ", "1", "=", "1"],
                ["1", " ", ">", " ", "2", " ", "1", ">=", "2", " ", "1", "<", "2", " ", "1", "<=", "2",
                    " ", "1", "=", "2"]);
        });

        it('should tokenizer error on invalid operator tokens', function () {
            _verifyTokenParseError("1++", "Error: Unexpected Operator Token ++ in query 1++");
            _verifyTokenParseError("1+=", "Error: Unexpected Operator Token += in query 1+=");
            _verifyTokenParseError("1 == 2", "Error: Unexpected Operator Token == in query 1 == 2");
        });

        //MYSQL_FUNCTIONS
        it('should tokenizer extract MATH function tokens', function () {
            _verifyAllTokens("!ABS(-2) > EXP(2,3)",
                ["!", "fn", "(", "-", "1", ")", " ", ">", " ", "fn", "(", "1", ",", "1", ")"],
                ["!", "ABS", "(", "-", "2", ")", " ", ">", " ", "EXP", "(", "2", ",", "3", ")"]);
        });

        it('should tokenizer functions be case insensitive', function () {
            _verifyAllTokens("ABS() abs()",
                ['fn', '(', ')', ' ', 'fn', '(', ')'],
                ["ABS", "(", ")", " ", "abs", "(", ")"]);
            // wierd mixed cases are not allowed for mysql functions. either all should be small case or big case.
            _verifyTokenParseError("aBs()", "Error: Unknown query function aBs in query aBs()");
        });

        it('should tokenizer functions string compare and if control test', function () {
            _verifyAllTokens("IF(1>2,STRCMP('x','y'),NULL)",
                ["fn", "(", "1", ">", "1", ",", "fn", "(", "'", ",", "'", ")", ",", "key", ")"],
                ["IF", "(", "1", ">", "2", ",", "STRCMP", "(", "'x'", ",", "'y'", ")", ",", "NULL", ")"]);
        });

        it('should tokenizer work for && AND OR || NOT !', function () {
            _verifyAllTokens("$.x=2 AND $.y.z=3 ||$",
                ["#", "=", "1", " ", "key", " ", "#", "=", "1", " ", "||", "#"],
                ["$.x", "=", "2", " ", "AND", " ", "$.y.z", "=", "3", " ", "||", "$"]);
            _verifyAllTokens("$.x=2 && $.y.z&3",
                ["#", "=", "1", " ", "&&", " ", "#", "&", "1"],
                ["$.x", "=", "2", " ", "&&", " ", "$.y.z", "&", "3"]);
            _verifyAllTokens("$.x=2 OR $.y.z=3",
                ["#", "=", "1", " ", "key", " ", "#", "=", "1"],
                ["$.x", "=", "2", " ", "OR", " ", "$.y.z", "=", "3"]);
            _verifyAllTokens("$.x=2 || $.y.z|3",
                ["#", "=", "1", " ", "||", " ", "#", "|", "1"],
                ["$.x", "=", "2", " ", "||", " ", "$.y.z", "|", "3"]);
            _verifyAllTokens("!($.x&&$.Y)",
                ['!', '(', '#', '&&', '#', ')'],
                ['!', '(', '$.x', '&&', '$.Y', ')' ]);
            _verifyAllTokens("NOT($.x AND $.Y)",
                ['key', '(', '#', ' ', 'key', ' ', '#', ')'],
                ['NOT', '(', '$.x', ' ', 'AND', ' ', '$.Y', ')']);
        });

        it('should tokenizer error on invalid variable tokens', function () {
            _verifyTokenParseError("x+1", "Error: Unknown query function x in query x+1");
            _verifyTokenParseError("$x+1", "Error: Invalid variable Token $x in query $x+1");
            _verifyTokenParseError("$.+1", "Error: Invalid variable Token $. in query $.+1");
            _verifyTokenParseError("$.x$+1", "Error: Invalid variable Token $.x$ in query $.x$+1");
        });
    });

    function _verifyQueryError(query, expectedErrorStr, indexFields) {
        let exceptionOccurred = false;
        try {
            Query.transformCocoToSQLQuery(query, indexFields);
        } catch (e) {
            exceptionOccurred = true;
            expect(e.toString()).to.eql(expectedErrorStr);
        }
        expect(exceptionOccurred).to.eql(true);
    }

    describe('Query Transformer tests', function () {
        it('should transformCocoToSQLQuery success cases without index field', function () {
            expect(Query.transformCocoToSQLQuery("")).to.eql("");
            expect(Query.transformCocoToSQLQuery("$.x")).to.eql('document->>"$.x"');
            expect(Query.transformCocoToSQLQuery("ABS($.x)")).to.eql('ABS(document->>"$.x")');
            expect(Query.transformCocoToSQLQuery("$.name LIKE 'va%'")).to.eql("document->>\"$.name\" LIKE 'va%'");
        });

        it('should transformCocoToSQLQuery success cases for json based searches', function () {
            expect(Query.transformCocoToSQLQuery(`JSON_CONTAINS($,'{"name": "v"}')`))
                .to.eql('JSON_CONTAINS(document,\'{"name": "v"}\')');
            expect(Query.transformCocoToSQLQuery(`JSON_CONTAINS($,'{"name": "v"}')`))
                .to.eql('JSON_CONTAINS(document,\'{"name": "v"}\')');
        });

        it('should transformCocoToSQLQuery success cases with index field', function () {
            expect(Query.transformCocoToSQLQuery("$.y>5 || $.x > 10 && $.price.tax = 18", ["x", "price.tax"]))
                .to.eql('document->>"$.y">5 || 9dd4e461268c8034f5c8564e155c67a6 > 10 && 6a1a731895e201b727851bc567ac1e8a = 18');
        });

        it('should transformCocoToSQLQuery error cases', function () {
            _verifyQueryError("x#", "Error: Unknown query function x in query x#");
        });

        it('should transformCocoToSQLQuery produce invalid output if sql commands in input', function () {
            _verifyQueryError("SELECT * FROM tab", "Error: Unknown query function SELECT in query SELECT * FROM tab");
        });

        it('should transformCocoToSQLQuery error cases with index field', function () {
            _verifyQueryError("x", "Error: invalid argument: useIndexForFields should be an array", "no array index");
        });
    });
});
