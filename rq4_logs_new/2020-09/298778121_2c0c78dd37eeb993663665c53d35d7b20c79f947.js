/**
 * Handle string sort
 * @param {object} a
 * @param {object} b
 * @param {string} key
 * @return {number}
 */
export const sortString = ({a, b, key}) => {
    const aStr = typeof a[key] === "string" ? a[key] : "";
    const bStr = typeof b[key] === "string" ? b[key] : "";

    return aStr.localeCompare(bStr);
};
/**
 * Handle number sort
 * @param {object} a
 * @param {object} b
 * @param {string} key
 * @return {number}
 */
export const sortNumber = ({a, b, key}) => {
    const aNum = typeof a[key] === "number" ? a[key] : 0;
    const bNum = typeof b[key] === "number" ? b[key] : 0;

    return aNum - bNum;
};