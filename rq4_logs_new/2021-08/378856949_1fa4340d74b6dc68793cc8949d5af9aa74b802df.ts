'use strict';

import toString from './to_string.ts';

/**
 * Converts string, as a whole, to lower case just like String#toLowerCase.
 * @param {string} str - The string to convert.
 * @returns {string}
 * @example
 * toLower("ABC"); // abc
 */
export default function toLower(str: string): string {
    return toString(str).toLowerCase();
}