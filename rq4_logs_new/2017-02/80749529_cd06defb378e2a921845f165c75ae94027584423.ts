declare namespace Objs {
    class Types {
        private constructor();
        static isDefined(value: any): boolean;
        static isArray(value: any): boolean;
        static isFunction(value: any): boolean;
        static isNative(value: any): boolean;
        static isPrimitive(value: any): boolean;
        static isDate(value: any): boolean;
        static areSameTypes(valueA: any, valueB: any): boolean;
        static getHashCode(value: any): number;
        private static getStringHashCode(value);
    }
}
declare namespace Objs.Cloning {
    class Cloner {
        private static checkAndPrepareTargetForCloning<T>(source, target);
        static shallowCloneTo<T>(source: T, target: Object): T;
        static deepCloneTo<T>(source: T, target: Object): T;
        static shallowClone<T>(value: T): T;
        private static cloneNonArray(value, deepCloning);
        static deepClone<T>(value: T): T;
        private static deepCloneWithCyclesHandling<T>(value, processedReferences);
        private static cloneNonArrayWithCyclesHandling(value, deepCloning, processedReferences);
    }
}
declare namespace Objs.Comparison {
    interface IEquivalenceComparisonOptions {
        isPropertyExcluded?: (propertyName: string, propertyValue: any) => boolean;
        ignoreMissingPropertyWhenUndefined?: boolean;
    }
    class Comparer {
        private static defaultClonesComparisonOptions;
        static areEquivalent<T>(valueA: T, valueB: T, comparisonOptions?: IEquivalenceComparisonOptions): boolean;
        private static storeProcessedEquivalenceComparison<T>(valueA, valueB, isEquivalent, processedReferences);
        private static checkForEquivalence<T>(valueA, valueB, processedReferences, comparisonOptions?);
    }
}
declare namespace Objs.Snapshots {
    interface ISnapshotterConfiguration {
        historyDepth: number;
        identificationKind: IdentificationKind;
        snapshotKind: SnapshotKind;
        propertyNameCasingKind: PropertyNameCasingKind;
    }
    enum IdentificationKind {
        Reference = 1,
        Id = 2,
    }
    enum SnapshotKind {
        DeepClone = 0,
        ShallowClone = 1,
    }
    enum PropertyNameCasingKind {
        LowerCamelCase = 0,
        UpperCamelCase = 1,
    }
    class Snapshotter {
        private snapshots;
        private configuration;
        private static defaultConfiguration;
        constructor(configuration?: ISnapshotterConfiguration);
        private getCasedIdPropertyName();
        clearAll(): Snapshotter;
        private getIdentifier(value);
        private getObjectDefinedErrorMessage(value);
        private ensureObjectDefinedOrThrow(value);
        private getHistory<T>(value);
        private getHistoryOrThrow<T>(value);
        has(value: Object): boolean;
        count(value: Object): number;
        isChanged(value: Object, comparisonOptions?: Objs.Comparison.IEquivalenceComparisonOptions): boolean;
        private clone<T>(value);
        private cloneTo<T>(source, target);
        save(value: Object): Snapshotter;
        clear(value: Object): Snapshotter;
        peek<T>(value: T): T;
        revert<T>(value: T): T;
    }
}