/// <reference path="../../typings/globals/jasmine/index.d.ts" />
/// <reference path="../../src/state.ts" />
describe("Objs.State.peek", () => {
    const notDefinedErrorMessage = "value is not defined";
    const primitiveErrorMessage = "could not act on a primitive value";
    const missingIdErrorMessage = "value does not defined an 'id' key"
    const notTrackedErrorMessage = "object is not tracked";
    let sutConfiguration: Objs.IStateConfiguration;

    const getSut = () => {
        if (sutConfiguration === undefined) {
            throw new Error("sut configuration not set");
        }
        return new Objs.State(sutConfiguration);
    };

    const withReferenceTracking = () => {
        sutConfiguration = {
            historyDepth: 1,
            pristineKind: Objs.PristineKind.DeepClone,
            propertyNameCasingKind: Objs.PropertyNameCasingKind.LowerCamelCase,
            trackingKind: Objs.TrackingKind.Reference
        };
    }

    const withIdTracking = () => {
        sutConfiguration = {
            historyDepth: 1,
            pristineKind: Objs.PristineKind.DeepClone,
            propertyNameCasingKind: Objs.PropertyNameCasingKind.LowerCamelCase,
            trackingKind: Objs.TrackingKind.Id
        };
    }

    beforeEach(() => {
        sutConfiguration = undefined as any as Objs.IStateConfiguration;
    });

    it("throw when trying to peek an untracked object in reference tracking mode", () => {
        withReferenceTracking();
        const sut = getSut();
        expect(sut.peek.bind(sut, {})).toThrowError(notTrackedErrorMessage);
    });

    it("throw when trying to peek an untracked object in id tracking mode", () => {
        withIdTracking();
        const sut = getSut();
        expect(sut.peek.bind(sut, { "id": 1 })).toThrowError(notTrackedErrorMessage);
    });

    it("throws when trying to peek null in reference tracking mode", () => {
        withReferenceTracking();
        const valueToPeek = null;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to peek null in id tracking mode", () => {
        withIdTracking();
        const valueToPeek = null;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to peek undefined in reference tracking mode", () => {
        withReferenceTracking();
        const valueToPeek = undefined;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to peek undefined in id tracking mode", () => {
        withIdTracking();
        const valueToPeek = undefined;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to peek a boolean in reference tracking mode", () => {
        withReferenceTracking();
        const valueToPeek = true;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to peek a boolean in id tracking mode", () => {
        withIdTracking();
        const valueToPeek = true;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(missingIdErrorMessage);
    });

    it("throws when trying to peek a number in reference tracking mode", () => {
        withReferenceTracking();
        const valueToPeek = 3.14;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to peek a number in id tracking mode", () => {
        withIdTracking();
        const valueToPeek = 3.14;
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(missingIdErrorMessage);
    });

    it("throws when trying to peek a string in reference tracking mode", () => {
        withReferenceTracking();
        const valueToPeek = "abc";
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to peek a string in id tracking mode", () => {
        withIdTracking();
        const valueToPeek = "abc";
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(missingIdErrorMessage);
    });

    it("does not throws when trying to peek a date in reference tracking mode", () => {
        withReferenceTracking();
        const valueToPeek = new Date();
        const sut = getSut();
        sut.save(valueToPeek);
        expect(sut.peek.bind(sut, valueToPeek)).not.toThrow();
    });

    it("throws when trying to peek a date in id tracking mode", () => {
        withIdTracking();
        const valueToPeek = new Date();
        const sut = getSut();
        expect(sut.peek.bind(sut, valueToPeek)).toThrowError(missingIdErrorMessage);
    });

    it("does not revert in reference tracking mode", () => {
        // arrange
        withReferenceTracking();
        const tracked = {
            "prop": "old"
        };
        const sut = getSut();
        sut.save(tracked);
        tracked.prop = "new";

        // act
        const actual = sut.peek(tracked);

        // assert
        expect(actual).not.toEqual(tracked);
    });

    it("does not revert in id tracking mode with same reference", () => {
        // arrange
        withIdTracking();
        const tracked = {
            "id": 1,
            "prop": "old"
        };
        const sut = getSut();
        sut.save(tracked);
        tracked.prop = "new";

        // act
        const actual = sut.peek(tracked);

        // assert
        expect(actual).not.toEqual(tracked);
    });

    it("does not revert to previous state in id tracking mode with another reference", () => {
        // arrange
        withIdTracking();
        const tracked = {
            "id": 1,
            "prop": "old"
        };
        const sut = getSut();
        sut.save(tracked);
        tracked.prop = "new";

        // act
        const actual = sut.peek({ "id": 1 });

        // assert
        expect(actual).toEqual({ "id": 1, "prop": "old" });
    });

    it("can peek multiple times in reference tracking mode", () => {
        // arrange
        withReferenceTracking();
        sutConfiguration.historyDepth = 3;
        const sut = getSut();
        const trackedObject = {
            "prop": "old"
        };
        sut.save(trackedObject);
        trackedObject.prop = "new";


        // act / assert
        let actual;
        actual = sut.peek(trackedObject);
        actual = sut.peek(trackedObject);
        actual = sut.peek(trackedObject);

        expect(actual).toEqual({ "prop": "old" });
    });
});