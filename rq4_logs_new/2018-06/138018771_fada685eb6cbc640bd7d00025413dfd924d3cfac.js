/**
 * 二分查找
 * @param {array} arr - 升序排列的数组
 * @param {number} data - 待查找的数据
 * @returns {*} - 如果没有找到，直接返回null，找到了就返回一个数组[12, index: 0, input: arr, count: 2]
 * 0：找到的值
 * index: 找到的值在arr中的索引
 * input: 自己输入待查找的字符串
 * count: 二分查找的次数
 */
var binSearch = function(arr, data) {
    if (!Array.isArray(arr)) {
        throw new Error('binSearch:The first argument excepts an array');
    }
    if (typeof data !== 'number') {
        throw new Error('binSearch:The second argument excepts a number')
    }

    var upperBound = arr.length - 1;
    var lowerBound = 0;
    var mid = 0;
    var count = 0;
    var matchedData = null;

    while (lowerBound <= upperBound) {
        count++;
        mid = Math.floor((lowerBound + upperBound) / 2);

        if (data < arr[mid]) {
            upperBound = mid - 1;
        } else if (data > arr[mid]) {
            lowerBound = mid + 1;
        } else {
            matchedData = arr[mid];
            break;
        }
    }

    // if not find data, return null
    if (matchedData === null) {
        return null;
    }
    var result = [matchedData];
    result.index = mid;
    result.input = arr;
    result.count = count;

    return result;
}

module.exports = binSearch;