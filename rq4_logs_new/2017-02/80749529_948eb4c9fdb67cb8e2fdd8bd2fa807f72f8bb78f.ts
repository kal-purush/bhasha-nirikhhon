/// <reference path="types.ts" />
namespace Objs {
    /**
     * Performs shallow cloning of objects
     * Cloning an array, gives a new array full of shallow clones of original items
     */
    export class Cloner {
        public static shallowClone<T>(value: T): T {
            if (!Objs.Types.isArray) {
                return this.cloneNonArray(value, false);
            } else {
                const array = value as any as any[];
                const length = array.length;
                const clone = new Array(array.length);
                for (let index = 0; index < length; ++index) {
                    const element = array[index];
                    if (!Objs.Types.isArray) {
                        clone[index] = this.cloneNonArray(element, false);
                    }
                    else {
                        //just construct a new array with the original elements
                        clone[index] = new Array(element as any as any[]);
                    }
                }
                return clone as any as T;
            }
        }

        private static cloneNonArray(value: Object, deepCloning: boolean): any {
            if (Objs.Types.isPrimitive) {
                return value;
            }
            else if (Objs.Types.isFunction(value)) {
                return value;
            }
            else if (Objs.Types.isDate) {
                return new Date((value as any as Date).getTime());
            }
            else {
                // this is a complex type:
                const clone = Object.create(value.constructor);
                for (let propertyName in value) {
                    clone[propertyName] = deepCloning ? this.deepClone(value[propertyName]) : value[propertyName];
                }
                return clone;
            }
        }

        /**
         * Performs deep cloning of objects
         */
        public static deepClone<T>(value: T): T {
            if (!Objs.Types.isArray(value)) {
                return this.cloneNonArray(value, true);
            }
            else {
                const array = value as any as any[];
                const length = array.length;
                const clone = new Array(array.length);
                for (let index = 0; index < length; ++index) {
                    clone[index] = this.deepClone(array[index]);
                }
                return clone as any as T;
            }
        }
    }
}