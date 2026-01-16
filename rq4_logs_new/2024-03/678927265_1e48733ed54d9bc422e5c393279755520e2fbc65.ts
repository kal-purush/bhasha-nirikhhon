/**
 * Truncates a string past the specified length, replaces the rest with '...'
 * @param {string} str The string to be evaulated and truncated
 * @param {number} num Length of string past which to truncate
 * @returns {string} Truncated string
 */
export const truncate = (str: string, num: number): string => {
    return str.length <= num
        ? str
        : `${str.slice(0, num)}...`;
};