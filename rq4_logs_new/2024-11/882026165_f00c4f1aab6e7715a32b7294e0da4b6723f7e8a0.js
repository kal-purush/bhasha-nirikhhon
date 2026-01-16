/**
 * Binary Search function
 * @param {number[]} sortedArray - An array of numbers sorted in ascending order.
 * @param {number} target - The number to search for.
 * @returns {number} The index of the target if found; otherwise, -1.
 */
function binarySearch(sortedArray, target) {
    let left = 0;
    let right = sortedArray.length - 1;

    while (left <= right) {
        const mid = Math.floor((left + right) / 2);
        if (sortedArray[mid] === target) {
            return mid;
        }
        if (sortedArray[mid] < target) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return -1;
}

// Export the function
module.exports = { binarySearch };