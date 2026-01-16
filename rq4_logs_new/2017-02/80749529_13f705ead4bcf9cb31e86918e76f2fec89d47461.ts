/// <reference path="../typings/globals/jasmine/index.d.ts" />
/// <reference path="../src/cloner.ts" />
describe("Objs.Cloner", () => {
    const sut = Objs.Cloner;
    it("shallowClone returns new array", () => {
        const originalArray: any[] = [];
        const actual = sut.shallowClone(originalArray);
        expect(actual).not.toBe(originalArray);
    });

    it("shallowClone returns new array with new items", () => {
        const originalArray: any[] = [{}];
        const actual = sut.shallowClone(originalArray);
        expect(actual[0]).not.toBe(originalArray[0]);
    });

    it("shallowClone returns new object", () => {
        const originalObject = {};
        const actual = sut.shallowClone(originalObject);
        expect(actual).not.toBe(originalObject);
    });

    it("shallowClone does not clone object properties", () => {
        const shared = {};
        const originalObject = {
            "shared" : shared
        };
        const actual = sut.shallowClone(originalObject);
        expect(actual.shared).toBe(shared);
    });

    it("deepClone returns new array", () => {
        const originalArray: any[] = [];
        const actual = sut.deepClone(originalArray);
        expect(actual).not.toBe(originalArray);
    });

    it("deepClone returns new array with new items", () => {
        const originalArray: any[] = [{}];
        const actual = sut.deepClone(originalArray);
        expect(actual[0]).not.toBe(originalArray[0]);
    });

    it("deepClone returns new object", () => {
        const originalObject = {};
        const actual = sut.deepClone(originalObject);
        expect(actual).not.toBe(originalObject);
    });
});