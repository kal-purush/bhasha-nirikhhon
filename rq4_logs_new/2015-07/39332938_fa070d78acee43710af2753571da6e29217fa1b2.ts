export module ES6DS {
        class KeyValuePair<K, V> {
            key: K;
            value: V;
            constructor(key: K, value: V) {
                this.key = key;
                this.value = value;
            }
        }
        export class Map<K, V> { // class MapDDD<K,V> implements Map
            // -------------- Fields -----------------------
            private keyAndValues: Array<KeyValuePair<K, V>>;
            // ---------------------------------------------
            constructor() {
                this.keyAndValues = [];
            }
            // --- Public Methods ---
            getKeysOfValue(value: V) {
                var keysToReturn: Array<K> = [];
                var valueToFind = value;
                this.keyAndValues.forEach(function (value: KeyValuePair<K, V>, index: number, array: KeyValuePair<K, V>[]): void {
                    if (value.value == valueToFind) {
                        keysToReturn.push(value.key);
                    }
                });
                return keysToReturn;
            }

            // Standard:
            clear(): void {
                this.keyAndValues = [];
            }
            delete(key: K): boolean {
                var found = false;
                this.keyAndValues.forEach(function (value: KeyValuePair<K, V>, index: number, array: KeyValuePair<K, V>[]): void {
                    if (found) return;
                    if (key == value.key) {
                        array = array.slice(0, index).concat(array.slice(index + 1));
                        found = true;
                    }
                });
                return found;
            }
            forEach(callbackfn: (value: V, key: K, map: Map<K, V>) => void, thisArg?: any): void {
                this.keyAndValues.forEach(function (value: KeyValuePair<K, V>, index: number, array: KeyValuePair<K, V>[]): void {
                    callbackfn.apply(thisArg, [value.value, value.key, this]);
                }, this);
            }
            get(key: K): V {
                var valueToReturn: V = undefined;
                this.keyAndValues.forEach(function (value: KeyValuePair<K, V>, index: number, array: KeyValuePair<K, V>[]): void {
                    if (valueToReturn !== undefined) return;
                    if (key == value.key) {
                        valueToReturn = value.value;
                    }
                });
                return valueToReturn;
            }
            has(key: K): boolean {
                var found = false;
                this.keyAndValues.forEach(function (value: KeyValuePair<K, V>, index: number, array: KeyValuePair<K, V>[]): void {
                    if (found) return;
                    if (key == value.key) {
                        found = true;
                    }
                });
                return found;
            }
            set(key: K, value: V): Map<K, V> {
                var found = false;
                var valueToSet = value;
                this.keyAndValues.forEach(function (value: KeyValuePair<K, V>, index: number, array: KeyValuePair<K, V>[]): void {
                    if (found) return;
                    if (key == value.key) {
                        found = true;
                        value.value = valueToSet;
                    }
                });
                if (!found) {
                    this.keyAndValues.push(new KeyValuePair<K, V>(key, valueToSet));
                }
                return this;
            }
            // ----------------------
            
            // Getters:
            // Standard:
            get size() {
                return this.keyAndValues.length;
            }
        }
    }