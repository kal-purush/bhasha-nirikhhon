//## Codewars 7q
//1. [more-than-one-way-to-call-a-function-or-skin-a-cat]
// (http://www.codewars.com/kata/more-than-one-way-to-call-a-function-or-skin-a-cat)
function sum(a, b){
    if (typeof b === "undefined") {
        return function (b) {
            return a + b;
        };
    }
    return a + b;
}

//2. [case-swapping](http://www.codewars.com/kata/case-swapping)

function swap(str){
    var arrTemp = str.split("");
    for (var i in arrTemp){
        if (arrTemp[i] >= "a"){
            arrTemp[i] = arrTemp[i].toUpperCase();
        } else {
            arrTemp[i] = arrTemp[i].toLowerCase();
        }
    }
    return arrTemp.join("");
}


//3. [descending-order](http://www.codewars.com/kata/descending-order)

function descendingOrder(n){
    var str ="";

    str += n;
    str = str.split("").sort().reverse().join(""); //
    return +str;
    }

//4. [complete-the-pattern-number-10-parallelogram]
// (http://www.codewars.com/kata/complete-the-pattern-number-10-parallelogram)
function pattern(n){
    var output="";
    if (n > 0){
        for (var i = 1; i <= n; i++){
            for (var key = 1; key <= n - i; key++){output += " ";}
            for (var key1 = 1; key1 <= n; key1++){output += key1 % 10;}
            for (var key2 = 1; key2 <= i - 1; key2++){output += " ";}
            output += "\n"
        }
    }
    return output.slice(0, output.length-1);
}
//5. [find-duplicates](http://www.codewars.com/kata/find-duplicates)

function duplicates(arr) {
    var arrTemp = [], count, arrGet = [], stringEnt = '';
    arrTemp = arr.slice();

    for (var counter = 0; counter < arrTemp.length; counter++)
    { count = 0;
        for (var counterInner = 0; counterInner < arr.length; counterInner++)
        { if (arr[counterInner] === arrTemp[counter]) count++;
        }
        if (count > 1) {arrGet.push(arrTemp[counter]);}
    }

    for (var count2 = 0; count2 < arrGet.length; count2++)
        if (stringEnt.indexOf(arrGet[count2]) < 0) {
            stringEnt = stringEnt + arrGet[count2] + " " ;
        }
    stringEnt = stringEnt.substring(0, stringEnt.length -1);

    if (arrGet.length > 0) {
        arrGet.length = 0;
        arrGet = stringEnt.split(' ');
        for (var i = 0; i < arrGet.length; i++){
            if (+arrGet[i]) arrGet[i] = +arrGet[i];
        }
    }
    return arrGet;
}

//6. [gradually-adding-parameters](http://www.codewars.com/kata/gradually-adding-parameters)
function add() {
    var args = [].slice.apply(arguments);
    if (args.length > 0){
        for (var i = 0; i < args.length; i++){
            args[i] = args[i] * (i + 1);
        }
        return args.reduce(function(a, b) {return a + b;})
    } else {
        return 0;
    }
}

//7. [reverseit-1](http://www.codewars.com/kata/reverseit-1)
function reverseIt(data){
    if (typeof (data) == "string") {
        var arr = data.split("");
        return arr.reverse().join("");
    } else {
        if (typeof (data) == "number"){
            var arr = data.toString().split("");
            return +arr.reverse().join("");
        } else return data;
    }
}

//8. [basic-js-calculating-averages](http://www.codewars.com/kata/basic-js-calculating-averages)
var Calculator = {
    average: function() {
        var args = [].slice.apply(arguments), sum = 0;
        if (args.length > 0){
            for (var i = 0; i < args.length; i++){
                sum += args[i];
            }
            sum = sum / args.length;
        }
        return sum;
    }
}


//## Codewars 6q

//1. [complete-fibonacci-series](http://www.codewars.com/kata/complete-fibonacci-series).
function fibonacci(n) {
    var result = [];
    if (n > 0){
        var a = 1, b = 0, x;
        for (i = 1; i <= n; i++) {
            x = a + b;
            a = b;
            b = x;
            result.push(a);
        }
    }
    return result;
}

//2. [detect-pangram](http://www.codewars.com/kata/detect-pangram)
function isPangram(string){
var Alphabet = {
    a : 0, b : 0, c : 0, d : 0, e : 0, f : 0,
    g : 0, h : 0, i : 0, j : 0, k : 0, l : 0,
    m : 0, n : 0, o : 0, p : 0, q : 0, r : 0,
    s : 0, t : 0, u : 0, v : 0, w : 0, x : 0,
    y : 0, z : 0
};
    var arr = string.toLowerCase().split("");

    for (var i in arr){
        if (Alphabet[arr[i]] !== undefined) {
            (Alphabet[arr[i]]++);
        }
    }

    for (var key in Alphabet){
        if (Alphabet[key] == 0) {
            return false;
        }
    }
    return true;
}
console.log(isPangram("The quick brown fox jumps over the lazy dog."));

//3. [multiples-of-3-and-5](http://www.codewars.com/kata/multiples-of-3-and-5)
function solution(number){
    var sum = 0;
    for(var i = 0; i<number; i++) {
        sum += (i % 3 == 0 || i % 5 == 0) ? i : 0;
    }
    return sum;
}

//4. [count-characters-in-your-string](http://www.codewars.com/kata/count-characters-in-your-string)
function count (string) {
    var arr = string.split("");
    var arr2={};
    for(i in arr){
        (arr2[arr[i]] != undefined) ? (arr2[arr[i]]++) : (arr2[arr[i]]=1);
    }

    return arr2;
}

//5. [typer-dot-js](http://www.codewars.com/kata/typer-dot-js)
function getClass(obj) {
    return {}.toString.call(obj).slice(8, -1);
}
var typer = (function() {

    return {
        isNumber: function(enterParametr){return ((getClass(enterParametr) == "Number") && (!isNaN(enterParametr)));},
        isString: function(enterParametr){return (getClass(enterParametr) == "String");},
        isArray: function(enterParametr){return (getClass(enterParametr) == "Array");},
        isFunction: function(enterParametr){return (typeof (enterParametr) == "function");},
        isDate:       function(enterParametr){return (getClass(enterParametr) == "Date");},
        isRegExp:     function(enterParametr){return (getClass(enterParametr) == "RegExp");},
        isBoolean:    function(enterParametr){return (getClass(enterParametr) == "Boolean");},
        isError:      function(enterParametr){return (getClass(enterParametr) == "Error");},
        isNull:       function(enterParametr){return (enterParametr === null);},
        isUndefined:  function(enterParametr){return (typeof (enterParametr) == "undefined");}
    };
}(null));
console.log(typer.isNull());