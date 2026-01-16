/// <reference path="../typings/lodash/lodash.d.ts" />
import {add, subtract} from "./math";
import times from "./math";
import { zip } from 'lodash';

var result = times(add(2, 3), subtract(5, 3));
var lodashResult = zip(['1', '2'], ['a', 'b']);

console.log(lodashResult);
document.write(result.toString());
