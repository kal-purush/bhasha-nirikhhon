//Chunk
var chunk = function (arr, size) {
    var temp = [];
    var main = [];
    var p = 0;
    for (var _i = 0, arr_1 = arr; _i < arr_1.length; _i++) {
        var element = arr_1[_i];
        if (p < size) {
            temp.push(element);
            p++;
        }
        if (p == size) {
            main.push(temp);
            temp = [];
            p = 0;
        }
    }
    main.push(temp);
    console.log(main);
};
chunk([1, 2, 3, 4, 5], 2);
//Reduce
var array1 = [1, 2, 3, 4];
var reducer = function (accumulator, currentValue) { return accumulator + currentValue; };
console.log(array1.reduce(reducer));
//FIlter
var words = ['spray', 'limit', 'elite', 'exuberant', 'destruction', 'present'];
var result = function (words) {
    var temp1 = [];
    for (var _i = 0, words_1 = words; _i < words_1.length; _i++) {
        var element = words_1[_i];
        if (element.length > 6) {
            temp1.push(element);
        }
    }
    console.log(temp1);
};
result(words);
//FInd
var array2 = [5, 12, 8, 130, 44];
var found = function (array2, cond, value) {
    for (var _i = 0, array2_1 = array2; _i < array2_1.length; _i++) {
        var ele = array2_1[_i];
        if (cond == '>') {
            if (ele > value) {
                return ele;
            }
        }
        if (cond == '<') {
            if (ele < value) {
                return ele;
            }
        }
        if (cond == '=') {
            if (ele == value) {
                return ele;
            }
        }
    }
};
console.log(found(array2, '>', 10));
//Sum
var array3 = [5, 12, 8, 130, 44];
var sum = function (array2) {
    var temp = 0;
    for (var _i = 0, array2_2 = array2; _i < array2_2.length; _i++) {
        var ele = array2_2[_i];
        temp = temp + ele;
    }
    return temp;
};
console.log(sum(array3));