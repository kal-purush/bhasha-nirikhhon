namespace Objs.Comparison {

    /** Options for behaviour of the clone comparison */
    export interface IEquivalenceComparisonOptions {
        /** When implemented, include or exclude properties check */
        isPropertyExcluded?: (propertyName: string, propertyValue: any) => boolean;
        /** Whether or not to ignore missing property in one object if the other one have that property set to undefined */
        ignoreMissingPropertyWhenUndefined?: boolean;
    }


    export class Comparer {

        private static defaultClonesComparisonOptions: IEquivalenceComparisonOptions = {
            isPropertyExcluded: undefined,
            ignoreMissingPropertyWhenUndefined: false
        };

        /** Check whether or not two object could be considered equivalent */
        public static areEquivalent<T>(
            valueA: T,
            valueB: T,
            comparisonOptions?: IEquivalenceComparisonOptions): boolean {

            comparisonOptions = comparisonOptions || this.defaultClonesComparisonOptions;
            if (valueA === valueB || (valueA === undefined && valueB === undefined) || (valueA === null && valueB === null)) {
                return true;
            }
            else {
                // one is null or undefined and not the other
                if (valueA === null || valueA === undefined) {
                    return false;
                }

                if (!Objs.Types.areSameTypes(valueA, valueB)) {
                    return false;
                }

                if (!Objs.Types.isArray(valueA)) {
                    const isNativeValueA = Objs.Types.isNative(valueA);
                    const isNativeValueB = Objs.Types.isNative(valueB);
                    if ((isNativeValueA && isNativeValueB)) {
                        // both native types (not object)
                        // just check against two equals dates here as they reference must be different but their values equal.
                        if (Objs.Types.isDate(valueA) && Objs.Types.isDate(valueB)) {
                            return (valueA as any as Date).getTime() === (valueB as any as Date).getTime();
                        }
                        return false;
                    }

                    // both are objects
                    const checkPropertyExclusion = comparisonOptions.isPropertyExcluded;
                    let keysA: string[] = checkPropertyExclusion
                        ? Object.keys(valueA).filter((key) => { return !checkPropertyExclusion(key, valueA[key]); })
                        : Object.keys(valueA);
                    let keysB: string[] = checkPropertyExclusion
                        ? Object.keys(valueB).filter((key) => { return !checkPropertyExclusion(key, valueB[key]); })
                        : Object.keys(valueB);

                    let keysALength = keysA.length;
                    let keysBLength = keysB.length;

                    if (keysALength !== keysBLength) {

                        if (!comparisonOptions.ignoreMissingPropertyWhenUndefined) {
                            return false;
                        }

                        //also discard undefined properties when the same property is not defined on the other side
                        keysA = keysA.filter((key) => {
                            if (valueA[key] === undefined) {
                                return valueB.hasOwnProperty(key);
                            }
                            return true;
                        });
                        keysB = keysB.filter((key) => {
                            if (valueB[key] === undefined) {
                                return valueA.hasOwnProperty(key);
                            }
                            return true;
                        });

                        keysALength = keysA.length;
                        keysBLength = keysB.length;

                        if (keysALength !== keysBLength) {
                            return false;
                        }

                    }

                    for (let index = 0; index < keysALength; ++index) {
                        const keyA = keysA[index];
                        if (keysB.indexOf(keyA) < 0) {
                            return false;
                        }
                        if (!this.areEquivalent(valueA[keyA], valueB[keyA], comparisonOptions)) {
                            return false;
                        }
                    }

                    return true;
                }
                else {
                    const arrayA = valueA as any as any[];
                    const arrayB = valueB as any as any[];
                    const arrayALenght = arrayA.length;
                    const arrayBLenght = arrayB.length;

                    if (arrayALenght !== arrayBLenght) {
                        return false;
                    }

                    for (let index = 0; index < arrayALenght; ++index) {
                        if (!this.areEquivalent(arrayA[index], arrayB[index], comparisonOptions)) {
                            return false;
                        }
                    }

                    return true;
                }
            }
        }
    }
}