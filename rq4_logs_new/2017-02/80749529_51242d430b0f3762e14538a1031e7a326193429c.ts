/// <reference path="../../typings/globals/jasmine/index.d.ts" />
/// <reference path="../../src/state.ts" />
describe("Objs.State.reset", () => {
    const sut = new Objs.State();

    const primitiveErrorMessage: string = "could not act on a primitive value";

    it("throws when trying to reset a boolean", () => {
        const valueToReset = true;
        expect(sut.reset.bind(sut, valueToReset)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to reset a number", () => {
        const valueToReset = 3.14;
        expect(sut.reset.bind(sut, valueToReset)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to reset a string", () => {
        const valueToReset = "abc";
        expect(sut.reset.bind(sut, valueToReset)).toThrowError(primitiveErrorMessage);
    });

    it("does not throws when trying to reset a date", () => {
        const valueToReset = new Date()
        expect(sut.reset.bind(sut, valueToReset)).not.toThrowError(primitiveErrorMessage);
    });
});