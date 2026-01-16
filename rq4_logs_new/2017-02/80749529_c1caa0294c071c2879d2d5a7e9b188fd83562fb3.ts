/// <reference path="../../typings/globals/jasmine/index.d.ts" />
/// <reference path="../../src/cloner.ts" />
describe("Objs.Cloner.areClones", () => {
    const sut = Objs.Cloner;

    it("return true for two equal booleans", () => {
        const actual = sut.areClones(true, true);
        expect(actual).toBe(true);
    });

    it("return false for two different booleans", () => {
        const actual = sut.areClones(true, false);
        expect(actual).not.toBe(true);
    });

    it("return true for two equal numbers", () => {
        const actual = sut.areClones(3.14, 3.14);
        expect(actual).toBe(true);
    });

    it("return false for two different numbers", () => {
        const actual = sut.areClones(3.14, 1.59);
        expect(actual).not.toBe(true);
    });

    it("return true for two equal strings", () => {
        const actual = sut.areClones("abc", "abc");
        expect(actual).toBe(true);
    });

    it("return false for two different strings", () => {
        const actual = sut.areClones("abc", "def");
        expect(actual).not.toBe(true);
    });

    it("return true for two equal dates", () => {
        const time = new Date().getTime();
        const actual = sut.areClones(new Date(time), new Date(time));
        expect(actual).toBe(true);
    });

    it("return false for two different dates", () => {
        const actual = sut.areClones(new Date(1), new Date(2));
        expect(actual).not.toBe(true);
    });

    it("return true for both empty arrays", () => {
        const actual = sut.areClones([], []);
        expect(actual).toBe(true);
    });

    it("return true for both empty objects", () => {
        const actual = sut.areClones({}, {});
        expect(actual).toBe(true);
    });
});