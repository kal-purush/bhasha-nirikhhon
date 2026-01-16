import {Scanner} from './scanner';
import {Parser} from './parser';
import {BaseParserHandler} from './parser-util';

const input = '[{"hello": "world", "a":4.2}, {"hello": [1, null, "\\"ok\\" with quotes"], "a": -1}, {"numbers": [1, 0, 0.0, -1, 3.14, -0.42, 0.123, 1e2, 4E3], "emptyString": ""}]';

const scanner = new Scanner();
const parserHandler = new BaseParserHandler();
const parser = new Parser(scanner, parserHandler);

let time = Date.now();
for (let i = 0; i < 100000; i++) {
    parser.parse(input);
}
const delta1 = Date.now() - time;

time = Date.now();
for (let i = 0; i < 100000; i++) {
    JSON.parse(input);
}
const delta2 = Date.now() - time;

console.log(delta1, delta2, delta1 / delta2);