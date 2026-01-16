

var isArray = function (value) {
    return value && 
        typeof value === 'object' && 
        value.constructor === Array;
};

var isArray = function (value) {
    return value &&
        typeof value === 'object' &&
        typeof value.length === 'number' &&
        typeof value.splice === 'function' &&
        !(value.propertyIsEnumerable('length'));
};

Array.dim = function (dimension, initialValue) {
    var array = [];
    for (var i = 0; i < dimension; i++) {
        array.push(initialValue);
    }
    return array;
};

Array.matrix = function (rows, cols, initialValue) {
    var matrix = [];
    for (var i = 0; i < rows; i++) {
        matrix[i] = [];
        for (var j = 0; j < cols; j++) {
            matrix[i][j] = initialValue;
        }
    }
    return matrix;
};

Array.identity = function (dimension) {
    var matrix = Array.matrix(dimension, dimension, 0);
    for (var i = 0; i < dimension; i++) {
        matrix[i][i] = 1;
    }
    return matrix;
};
