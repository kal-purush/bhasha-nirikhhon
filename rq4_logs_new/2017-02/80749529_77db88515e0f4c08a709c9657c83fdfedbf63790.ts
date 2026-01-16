/// <reference path="../typings/globals/jasmine/index.d.ts" />
/// <reference path="../src/state.ts" />
describe("Objs.State", () => {
    const sut = new Objs.State();

    it("isChanged returns false if tracked object properties have not changed", () => {
        // arrange
        const trackedObject = {
            "prop" : "old"
        };
        sut.track(trackedObject);
        trackedObject.prop = "old";

        // act
        const actual = sut.isChanged(trackedObject);

        // assert
        expect(actual).not.toBe(true);
    });

    it("isChanged returns true if tracked object properties have changed", () => {
        // arrange
        const trackedObject = {
            "prop" : "old"
        };
        sut.track(trackedObject);
        trackedObject.prop = "new";

        // act
        const actual = sut.isChanged(trackedObject);

        // assert
        expect(actual).toBe(true);
    });

    it("isChanged returns true if tracked object array properties elements have changed", () => {
        // arrange
        const trackedObject = {
            "prop" : [3.14]
        };
        sut.track(trackedObject);
        trackedObject.prop[0] = 1.59;

        // act
        const actual = sut.isChanged(trackedObject);

        // assert
        expect(actual).toBe(true);
    });

    it("isChanged returns true if tracked object nested array elements have changed", () => {
        // arrange
        const trackedObject = {
            "nested" : [3.14]
        };
        sut.track(trackedObject);
        trackedObject.nested[0] = 1.59;

        // act
        const actual = sut.isChanged(trackedObject);

        // assert
        expect(actual).toBe(true);
    });

    it("isChanged returns true if tracked object nested array elements properties have changed", () => {
        // arrange
        const trackedObject = {
            "nested" : [{"prop":3.14}]
        };
        sut.track(trackedObject);
        trackedObject.nested[0].prop = 1.59;

        // act
        const actual = sut.isChanged(trackedObject);

        // assert
        expect(actual).toBe(true);
    });
});