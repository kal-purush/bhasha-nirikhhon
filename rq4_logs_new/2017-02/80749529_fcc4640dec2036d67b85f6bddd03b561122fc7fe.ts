/// <reference path="../../typings/globals/jasmine/index.d.ts" />
/// <reference path="../../src/cloner.ts" />
describe("Objs.Cloning.Cloner.deepCloneTo", () => {
    const sourceNotDefinedErrorMessage = "source is not defined";
    const targetNotDefinedErrorMessage = "target is not defined";
    const primitiveErrorMessage = "could not act on a primitive value";
    const typesVariationErrorMessage = "source and target types varies";
    const sameInstancesErrorMessage = "could not act on same instances";
    const sut = Objs.Cloning.Cloner;

    it("throws if source and target types varies", () => {
        expect(sut.deepCloneTo.bind(sut, {}, [])).toThrowError(typesVariationErrorMessage);
    });

    it("throws if source is undefined", () => {
        expect(sut.deepCloneTo.bind(sut, undefined, {})).toThrowError(sourceNotDefinedErrorMessage);
    });

    it("throws if source is null", () => {
        expect(sut.deepCloneTo.bind(sut, null, {})).toThrowError(sourceNotDefinedErrorMessage);
    });

    it("throws if target is undefined", () => {
        expect(sut.deepCloneTo.bind(sut, {}, undefined)).toThrowError(targetNotDefinedErrorMessage);
    });

    it("throws if target is null", () => {
        expect(sut.deepCloneTo.bind(sut, {}, null)).toThrowError(targetNotDefinedErrorMessage);
    });

    it("throws if source and target are the same instance", () => {
        const source = {};
        const target = source;
        expect(sut.deepCloneTo.bind(sut, source, target)).toThrowError(sameInstancesErrorMessage);
    });

    it("throws if cloning booleans", () => {
        expect(sut.deepCloneTo.bind(sut, true, false)).toThrowError(primitiveErrorMessage);
    });

    it("throws if cloning numbers", () => {
        expect(sut.deepCloneTo.bind(sut, 1.59, 3.14)).toThrowError(primitiveErrorMessage);
    });

    it("throws if cloning strings", () => {
        expect(sut.deepCloneTo.bind(sut, "", "")).toThrowError(primitiveErrorMessage);
    });

    it("does not throw if cloning dates", () => {
        expect(sut.deepCloneTo.bind(sut, new Date(), new Date())).not.toThrow();
    });

    it("handles both empty arrays", () => {
        const source: any[] = [];
        const target: any[] = [];
        sut.deepCloneTo(source, target);
        expect(target).toEqual(source);
    });

    it("removes additional element in target array", () => {
        const source: any[] = ["base"];
        const target: any[] = ["base", "additional"];
        sut.deepCloneTo(source, target);
        expect(target).toEqual(source);
    });

    it("add missing element in target array", () => {
        const source: any[] = ["base", "additional"];
        const target: any[] = ["base"];
        sut.deepCloneTo(source, target);
        expect(target).toEqual(source);
    });

    it("clones object properties", () => {
        const shared = {};
        const source = {
            "shared": shared
        };
        const target = {};
        const actual = sut.deepCloneTo(source, target);
        expect(actual.shared).not.toBe(shared);
    });

    it("clones object nested properties", () => {
        const shared = {};
        const source = {
            "nested": {
                "shared": shared
            }
        };
        const target = {};
        const actual = sut.deepCloneTo(source, target);
        expect(actual.nested.shared).not.toBe(shared);
    });
});