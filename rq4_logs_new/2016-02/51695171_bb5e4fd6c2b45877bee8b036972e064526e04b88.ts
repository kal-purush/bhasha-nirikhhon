//  ag-lazy-ts 0.1.0
//  Lazy initialization for TypeScript and JavaScript.
//  Author: Alexey Gorshkov
//  Project: https://github.com/agorshkov23/ag23-lazy-ts
//  License: MIT

/**Flag to globally disable lazy initialization. */
declare var LAZY_OFF: boolean;

module ag23 {
    /**Provides support for lazy initialization. */
    export class Lazy<T>{
        /**Initializes a new instance of the Lazy<T> class. When lazy initialization occurs, the specified initialization function is used. */
        constructor(valueFactory: () => T) {
            if (!(valueFactory instanceof Function))
                throw new Error("Argument valueFactory = " + valueFactory + " is not function");

            this._valueFactory = valueFactory;
            if (LAZY_OFF === true)
                this.createValue();
        }

        /**Gets a value that indicates whether a value has been created for this Lazy<T> instance. */
        get isValueCreated() {
            return this._isValueCreated;
        }

        /**Gets the lazily initialized value of the current Lazy<T> instance. */
        get value() {
            if (this.isValueCreated)
                return this._value;

            this.createValue();
            return this._value;
        }

        /**Creates and returns a string representation of the Lazy<T>. Value property for this instance. */
        toString() {
            return this.isValueCreated
                ? this.value.toString()
                : "Value is not created.";
        }

        private _isValueCreated = false;
        private _valueFactory: () => T;
        private _value: T;

        private createValue() {
            if (this._isValueCreated)
                return;
            this._isValueCreated = true;
            this._value = this._valueFactory();
        }
    }
}