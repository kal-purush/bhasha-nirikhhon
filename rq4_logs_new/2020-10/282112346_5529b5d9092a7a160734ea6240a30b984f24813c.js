/**
 * array2をarray1にマージします
 * 
 * @param {Array<number>} array1 array2が入る十分な空きのある配列
 * @param {Array<number>} array2
 * @returns {void}
 */
function mergeArray(array1, array2) {
    if (array1.length === 0) {
        while(array2.length !== 0) {
            array1.push(array2.pop());
        }
        return;
    }
    var array1Cursor = array1.length - 1;
    while (array2.length !== 0) {
        const array1Max = array1[array1Cursor];
        const array2Max = array2[array2.length - 1];
        if (array2Max >= array1Max) {
            array1[array1Cursor + array2.length] = array2.pop();
        } else {
            array1[array1Cursor + array2.length] = array1Max;
            array1Cursor--;
        }
        if (array1Cursor === -1) {
            while(array2.length !== 0) {
                array1[array2.length - 1] = array2.pop();
            }
        }
    }
}

module.exports = {
    mergeArray,
};