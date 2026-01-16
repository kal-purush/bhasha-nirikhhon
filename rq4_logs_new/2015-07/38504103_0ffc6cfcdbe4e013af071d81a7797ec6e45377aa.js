/**
 * Created by mzimmerman on 7/5/15.
 */
var bases = ['A','C','G','T'],
    compBases = {
        A:'T',
        T:'A',
        G:'C',
        C:'G'
    }

var reverseComplement = function(pattern) {
    var res = "";
    for (var i = 0; i < pattern.length; i++) {
        res = compBases[pattern[i]] + res;
    }
    return res;
}

var patternToInt = function(pattern) {
    var res = 0;
    for (var i = 0; i < pattern.length; i++) {
        res += bases.indexOf(pattern[pattern.length - i - 1]) * Math.pow(4,i);
    }
    return res;
}

var intToPattern = function(n,k) {
    var res = "",
        base;
    for (var i = 0; i < k; i++) {
        base = bases[Math.floor(n / Math.pow(4,i)) % 4] || 'A';
        res = base + res;
    }
    return res;
}

module.exports = {
    reverseComp: reverseComplement,
    intToPattern: intToPattern,
    patternToInt: patternToInt
}