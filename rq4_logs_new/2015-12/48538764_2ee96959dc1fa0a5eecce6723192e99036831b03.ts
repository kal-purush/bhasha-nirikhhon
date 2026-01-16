/// <reference path="../typings/tsd.d.ts" />
import Add from "../lib/Add";
import Range from "../lib/Range";
import Item from "../lib/Item";

describe("Basic Tests", () => {
    it("add(1, 1)", done => {
        let result;
        let it = new Add(new Item([1]), new Item([1]));
        it.subscribe({
            next: item => {
                result = item;
            },
            return: () => {
                expect(result).toEqual(2);
                done();
            },
            throw: error => { throw error; }
        });
    });

    it("range(1, 10)", done => {
        let items = [];
        let it = new Range(new Item([1]), new Item([10]));
        it.subscribe({
            next: item => {
                items.push(item);
            },
            return: () => {
                expect(items).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
                done();
            },
            throw: error => { throw error; }
        });
    });
});


