function sortBy(array, fn) {
    const result = array.sort(function compareFn(a, b){
        return fn(a) - fn(b);
    })
    return result;
}


module.exports = sortBy;