/// <reference path="../../typings/globals/jasmine/index.d.ts" />
/// <reference path="../../src/snapshotter.ts" />
describe("Objs.Snapshots.Snapshotter.has", () => {
    const notDefinedErrorMessage = "value is not defined";
    const primitiveErrorMessage = "could not act on a primitive value";
    const missingIdErrorMessage = "value does not defined an 'id' property";
    let sutConfiguration: Objs.Snapshots.ISnapshotterConfiguration;

    const getSut = () => {
        if (sutConfiguration === undefined) {
            throw new Error("sut configuration not set");
        }
        return new Objs.Snapshots.Snapshotter(sutConfiguration);
    };

    const withReferenceTracking = () => {
        sutConfiguration = {
            historyDepth: 1,
            snapshotKind: Objs.Snapshots.SnapshotKind.DeepClone,
            propertyNameCasingKind: Objs.Snapshots.PropertyNameCasingKind.LowerCamelCase,
            identificationKind: Objs.Snapshots.IdentificationKind.Reference
        };
    }

    const withIdTracking = () => {
        sutConfiguration = {
            historyDepth: 1,
            snapshotKind: Objs.Snapshots.SnapshotKind.DeepClone,
            propertyNameCasingKind: Objs.Snapshots.PropertyNameCasingKind.LowerCamelCase,
            identificationKind: Objs.Snapshots.IdentificationKind.Id
        };
    }

    beforeEach(() => {
        sutConfiguration = undefined as any as Objs.Snapshots.ISnapshotterConfiguration;
    });

    it("does not throw when trying to check an untracked object in reference tracking mode", () => {
        withReferenceTracking();
        const sut = getSut();
        expect(sut.has.bind(sut, {})).not.toThrow();
    });

    it("does not throw when trying to check an untracked object in id tracking mode", () => {
        withIdTracking();
        const sut = getSut();
        expect(sut.has.bind(sut, { "id": 1 })).not.toThrow();
    });

    it("throws when trying to check null in reference tracking mode", () => {
        withReferenceTracking();
        const valueToSave = null;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToSave)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to check null in id tracking mode", () => {
        withIdTracking();
        const valueToSave = null;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToSave)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to check undefined in reference tracking mode", () => {
        withReferenceTracking();
        const valueToSave = undefined;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToSave)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to check undefined in id tracking tracking mode", () => {
        withIdTracking();
        const valueToSave = undefined;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToSave)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to check a boolean in reference tracking mode", () => {
        withReferenceTracking();
        const valueToCheck = true;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to check a boolean in id tracking mode", () => {
        withIdTracking();
        const valueToCheck = true;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).toThrowError(missingIdErrorMessage);
    });

    it("throws when trying to check a number in reference tracking mode", () => {
        withReferenceTracking();
        const valueToCheck = 3.14;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to check a number in id tracking mode", () => {
        withIdTracking();
        const valueToCheck = 3.14;
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).toThrowError(missingIdErrorMessage);
    });

    it("throws when trying to check a string in reference tracking mode", () => {
        withReferenceTracking();
        const valueToCheck = "abc";
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to check a string in id tracking mode", () => {
        withIdTracking();
        const valueToCheck = "abc";
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).toThrowError(missingIdErrorMessage);
    });

    it("does not throws when trying to check a missing date in reference tracking mode", () => {
        withReferenceTracking();
        const valueToCheck = new Date();
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).not.toThrow();
    });

    it("does not throws when trying to check a present date in reference tracking mode", () => {
        withReferenceTracking();
        const valueToCheck = new Date();
        const sut = getSut();
        sut.save(valueToCheck);
        expect(sut.has.bind(sut, valueToCheck)).not.toThrow();
    });

    it("throws when trying to check a date in id tracking mode", () => {
        withIdTracking();
        const valueToCheck = new Date();
        const sut = getSut();
        expect(sut.has.bind(sut, valueToCheck)).toThrowError(missingIdErrorMessage);
    });

    it("returns true if tracked object has one shapshot in reference tracking mode", () => {
        // arrange
        withReferenceTracking();
        const trackedObject = {
            "prop": "old"
        };
        const sut = getSut();
        sut.save(trackedObject);

        // act
        const actual = sut.has(trackedObject);

        // assert
        expect(actual).toBe(true);
    });

    it("returns true if tracked object has one shapshot in id tracking mode", () => {
        // arrange
        withIdTracking();
        const trackedObject = {
            "id": 1
        };
        const sut = getSut();
        sut.save(trackedObject);

        // act
        const actual = sut.has(trackedObject);

        // assert
        expect(actual).toBe(true);
    });

    it("returns true if tracked object has many shapshot in reference tracking mode", () => {
        // arrange
        withReferenceTracking();
        const trackedObject = {
            "prop": "snapshot1"
        };
        const sut = getSut();
        sut.save(trackedObject);
        trackedObject.prop = "snapshot2";
        sut.save(trackedObject);
        trackedObject.prop = "snapshot3";
        sut.save(trackedObject);

        // act
        const actual = sut.has(trackedObject);

        // assert
        expect(actual).toBe(true);
    });

    it("returns true if tracked object has many shapshot in id tracking mode", () => {
        // arrange
        withIdTracking();
        const trackedObject = {
            "id": 1,
            "prop": "snapshot1"
        };
        const sut = getSut();
        sut.save(trackedObject);
        trackedObject.prop = "snapshot2";
        sut.save(trackedObject);
        trackedObject.prop = "snapshot3";
        sut.save(trackedObject);

        // act
        const actual = sut.has(trackedObject);

        // assert
        expect(actual).toBe(true);
    });

    it("returns false if tracked object has no shapshots in reference tracking mode", () => {
        withReferenceTracking();
        expect(getSut().has({})).toBe(false);
    });

    it("returns false if tracked object has no shapshots in id tracking mode", () => {
        withIdTracking();
        expect(getSut().has({ "id": 1 })).toBe(false);
    });
});