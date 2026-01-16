/// <reference path="../typings/tsd.d.ts" />
import Seq from "../lib/inputs/AsyncSequence";
import Sequence from "../lib/inputs/Sequence";
import Stream from "../lib/Stream";

describe("Basic Tests", () => {
/*
    it("async seq", done => {
        let it = new Stream<number>(Sequence.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let items = [];
        it.subscribe({
            next: item => {
                items.push(item);
            },
            return: () => {
                expect(items).toEqual([1, 2, 3, 4, 5]);
                done();
            },
            throw: error => {
                console.error(error);
                expect(false).toBe(true);
                done();
            }
        });
    });

    it("take(5)", done => {
        let it = new Stream<number>(Sequence.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let items = [];
        it.forEach(item => {
                items.push(item);
            })
            .take(5)
            .return(() => {
                expect(items).toEqual([1, 2, 3, 4, 5]);
                done();
            });
    });


    it("take(5, 2)", done => {
        let it = new Stream<number>(Sequence.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let items = [];
        it
            .take(5)
            .map(item => item * item)
            .take(2)
            .forEach(item => items.push(item))
            .return(() => {
                expect(items).toEqual([1, 4]);
                done();
            });
    });

    it("filter(i > 2)", done => {
        let it = new Stream<number>(Sequence.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let items = [];
        it.take(5).filter(item => {
            return item > 2;
        }).forEach(item => {
            items.push(item);
        }).return(() => {
            expect(items).toEqual([3, 4, 5]);
            done();
        });
    });

    it("map(i * i)", done => {
        let it = new Stream<number>(Sequence.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let items = [];
        it
            .take(5)
            .filter(item => item > 2)
            .map(item => item * item)
            .filter(item => item > 24)
            .forEach(item => items.push(item))
            .return(() => {
                expect(items).toEqual([25]);
                done();
            });
    });

    it("map(i * i)", done => {
        let it = new Stream<number>(Sequence.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let items = [];
        it
            .filter(item => item > 4)
            .map(item => item * item)
            .filter(item => item > 24)
            .take(2)
            .skip(1)
            .forEach(item => items.push(item))
            .return(() => {
                expect(items).toEqual([36]);
                done();
            });
    });
    */
});