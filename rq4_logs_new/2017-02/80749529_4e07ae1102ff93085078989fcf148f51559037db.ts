/// <reference path="../../typings/globals/jasmine/index.d.ts" />
/// <reference path="../../src/snapshotter.ts" />
describe("Objs.Snapshots.Snapshotter.clear", () => {
    const notDefinedErrorMessage = "value is not defined";
    const primitiveErrorMessage = "could not act on a primitive value";
    const missingIdErrorMessage = "value does not defined an 'id' property"
    const notTrackedErrorMessage = "value has no snapshots";
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

    it("throw when trying to clear an untracked object in reference tracking mode", () => {
        withReferenceTracking();
        const sut = getSut();
        expect(sut.clear.bind(sut, {})).toThrowError(notTrackedErrorMessage);
    });

    it("throw when trying to clear an untracked object in id tracking mode", () => {
        withIdTracking();
        const sut = getSut();
        expect(sut.clear.bind(sut, { "id": 1 })).toThrowError(notTrackedErrorMessage);
    });

    it("throws when trying to clear null in reference tracking mode", () => {
        withReferenceTracking();
        const valueToReset = null;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to clear null in id tracking mode", () => {
        withIdTracking();
        const valueToReset = null;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to clear undefined in reference tracking mode", () => {
        withReferenceTracking();
        const valueToReset = undefined;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to clear undefined in id tracking mode", () => {
        withIdTracking();
        const valueToReset = undefined;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(notDefinedErrorMessage);
    });

    it("throws when trying to clear a boolean in reference tracking mode", () => {
        withReferenceTracking();
        const valueToReset = true;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to clear a boolean in id tracking mode", () => {
        withIdTracking();
        const valueToReset = true;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(missingIdErrorMessage);
    });

    it("throws when trying to clear a number in reference tracking mode", () => {
        withReferenceTracking();
        const valueToReset = 3.14;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to clear a number in id tracking mode", () => {
        withIdTracking();
        const valueToReset = 3.14;
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(missingIdErrorMessage);
    });

    it("throws when trying to clear a string in reference tracking mode", () => {
        withReferenceTracking();
        const valueToReset = "abc";
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to clear a string in id tracking mode", () => {
        withIdTracking();
        const valueToReset = "abc";
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(missingIdErrorMessage);
    });

    it("does not throws when trying to clear a date in reference tracking mode", () => {
        withReferenceTracking();
        const valueToReset = new Date();
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).not.toThrowError(primitiveErrorMessage);
    });

    it("throws when trying to clear a date in id tracking mode", () => {
        withIdTracking();
        const valueToReset = new Date();
        const sut = getSut();
        expect(sut.clear.bind(sut, valueToReset)).toThrowError(missingIdErrorMessage);
    });
});