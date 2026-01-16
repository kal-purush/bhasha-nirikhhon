import {Scanner} from './scanner';
import {Parser, IParserHandler} from './parser';
import {ParserLoggerHandler, ParserCreateObjectHandler, ParserCompositeHandler} from './parser-util';

const input = '[{"hello": "world", "a":4.2}, {"hello": [1, null, "\\"ok\\" with quotes"], "a": -1}, {"numbers": [1, 0, 0., -1, 3.14, -0.42, 0.123, 1e2, 4E3], "emptyString": ""}]';

const scanner = new Scanner();
const parserCreateObjectHandler = new ParserCreateObjectHandler();
const parserHandler = new ParserCompositeHandler()
                        .add(new ParserLoggerHandler())
                        .add(parserCreateObjectHandler);
const parser = new Parser(scanner, parserHandler);

parser.parse(input);

console.log(parserCreateObjectHandler.value);