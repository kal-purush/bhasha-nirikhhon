import { Schema } from "../serialize/Schema.js";
import { DecodeResult, jsonEncodeWith, jsonDecodeWith } from "../serialize/Serialize.js";

export namespace Storage {
    /**
     * All of the following can indicate unsupported local storage:
     * - window.localStorage is undefined/null
     * - window.localStorage exists, but accessing it throws an exception
     * - window.localStorage is accessible, but has a quota of 0, so setItem
     *   calls will always fail
     */
    export const isSupported: boolean = (() => {
        let storage;
        try {
            storage = window.localStorage;
            const key = "__test_ls";
            storage.setItem(key, "null");
            storage.removeItem(key);
            return true;
        } catch (e) {
            if (!storage || storage.length === 0) {
                return false;
            }
            if (e instanceof DOMException) {
                return e.code === 22 || e.name === "QuotaExceededError"
                // Firefox compatibility
                || e.code === 1014 || e.name === "NS_ERROR_DOM_QUOTA_REACHED";
            }
            return false;
        }
    })();

    /**
     * Check if an item entry exists in local storage
     */
    export function hasItem(key: string): boolean {
        return isSupported && Object.prototype.hasOwnProperty.call(window.localStorage, key);
    }

    /**
     * Store an item to local storage, encoding the value as JSON according to
     * the provided schema. Returns 'true' if the storage succeeded, or 'false'
     * otherwise.
     */
    export function saveItem<T>(key: string, value: T, schema: Schema<T, any>): boolean {
        if (isSupported) {
            const json = jsonEncodeWith(value, schema);
            window.localStorage.setItem(key, json);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Queries local storage for the value associated with a given key. Returns
     * 'undefined' if the key was not present, or if the key had an associated
     * value, attempt to decode the value from JSON according to the provided
     * schema and return a 'DecodeResult<T>'.
     */
    export function loadItem<T>(key: string, schema: Schema<T, any>): DecodeResult<T> | undefined {
        if (isSupported) {
            const json = window.localStorage.getItem(key);
            if (json !== null) {
                return jsonDecodeWith(json, schema);
            } else {
                return undefined;
            }
        } else {
            return undefined;
        }
    }

    /**
     * Removes a key/value pair from local storage
     */
    export function removeItem(key: string): void {
        if (isSupported) {
            window.localStorage.removeItem(key);
        }
    }

    /**
     * Clears all key/value pairs from local storage.
     */
    export function clearAllItems(): void {
        if (isSupported) {
            window.localStorage.clear();
        }
    }
}