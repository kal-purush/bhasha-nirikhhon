/**
 * Typescript test file.
 * We use it to test our compiled module
 *
 * - We should get compilation error
 * - We should get autocompletion based on d.ts file
 * - We should get hint error on several IDE
 *
 * example: tsc --out main.js main.ts
 */

///<reference path="./dist/src/prime.d.ts"/>

import { PrimeService } from './dist/src/prime';
let primeService = new PrimeService()

// error TS2345: Argument of type 'string' is not assignable to parameter of type 'number'.
console.log(primeService.isPrime('a'))