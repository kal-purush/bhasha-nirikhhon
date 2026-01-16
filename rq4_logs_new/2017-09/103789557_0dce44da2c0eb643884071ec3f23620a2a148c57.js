/**
 * Created by jsalwitz on 9/16/17.
 */

function sum(arr) {
    return arr.reduce(function (a, b) {
        return a + b
    }, 0)
}

module.exports.sum = sum