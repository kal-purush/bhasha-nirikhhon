/// <reference path="../typings/tsd.d.ts" />
import Seq from "../lib/inputs/AsyncSequence";
import AsyncSequence from "../lib/inputs/AsyncSequence";
import Sequence from "../lib/inputs/Sequence";
import Stream from "../lib/Stream";

describe("Basic Tests", () => {

    it("combine 1", done => {
        let delayPromise = function(ms, value) {
            return new Promise(resolve => setTimeout(() => resolve(value), ms));
        };
        let allTheIntegers = function*() {
            let i = 0;
            while(true) {
                yield delayPromise(25, ++i);
            }
        };
        let results = [];
        let left = new Stream<number>(new AsyncSequence(allTheIntegers)).take(5);
        let right = new Stream<number>(new AsyncSequence(allTheIntegers)).take(5);
        let add = (left, right) => left + right;
        Stream
            .combine(add, [left, right])
            .forEach(result => results.push(result))
            .return(() => {
                expect(results).toEqual([2, 4, 6, 8, 10]);
                done();
            });
    });
/*
    it("combine 2", done => {
        let results = [];
        let left = new Stream(Sequence.from([1, 2, 3, 4, 5]));
        let right = new Stream(Sequence.from([1, 2, 3, 4, 5]));
        let add = (left, right) => left + right;
        Stream
            .combine(add, [left, right])
            .forEach(result => results.push(result))
            .return(() => {
                expect(results).toEqual([2, 4, 6, 8, 10]);
                done();
            });
    });
    */
});