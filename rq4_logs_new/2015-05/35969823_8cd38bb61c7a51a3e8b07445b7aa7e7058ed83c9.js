Array.prototype.each = function(fn) {
    var ary = [];
    var args = Array.prototype.slice.call(arguments);
    for (var i = 0; i < this.length; i++) {
        var res = fn.apply(this, [this[i], i].concat(args));
        if (res != null) {
            ary.push(res);
        }
    }
    return ary;
}
//contains:在数组的原型链上添加判断数组中是否包含某一个元素的方法
Array.prototype.contains = function(zhi) {
    var count = 0;
    for (var i = 0; i < this.length; i++) {
        if (this[i] == zhi) {
            count++;
            break;
        }
    }
    return count;
}
//distinct:在数组的原型链上添加数组去除重复,但是不损坏原先的数组
Array.prototype.distinct = function() {
    var array = this;
    for (var i = 0; i < array.length - 1; i++) {
        for (var j = i + 1; j < array.length;) {
            if (array[i] == array[j]) {
                array.splice(j, 1);
            } else {
                j++;
            }
        }
    }
    return array;
}
//intersection:在数组的类方法中添加获取数组交集的方法
Array.intersection = function() {
    var ary = [];
    var ary1 = [];
    if (arguments.length >= 2) {
        for (var i = 0; i < arguments.length - 1; i++) {
            var arg = arguments;
            if (ary1 && ary1.length == 0) {
                ary1 = arg[i].distinct().each(function(zhi) {
                    return arg[i + 1].contains(zhi) ? zhi: null;
                });
            } else {
                ary1 = ary1.distinct().each(function(zhi) {
                    return arg[i + 1].contains(zhi) ? zhi: null;
                });
            }
        }
    }
    ary = ary1;
    ary1 = null;
    return ary;
}
//difference:在数组的类方法中添加获取数组差集的方法
Array.difference = function() {
    var ary = [];
    var ary1 = [];
    if (arguments.length >= 2) {
        for (var i = 0; i < arguments.length - 1; i++) {
            var arg = arguments;
            if (ary1 && ary1.length == 0) {
                ary1 = arg[i].distinct().each(function(zhi) {
                    return arg[i + 1].contains(zhi) ? null: zhi;
                });
            } else {
                ary1 = ary1.distinct().each(function(zhi) {
                    return arg[i + 1].contains(zhi) ? null: zhi;
                });
            }
        }
    }
    ary = ary1;
    ary1 = null;
    return ary;
}
//union:在数组的类方法中添加获取数组并集的方法
Array.union = function() {
    var ary = [];
    var ary1 = [];
    if (arguments.length >= 2) {
        for (var i = 0; i < arguments.length - 1; i++) {
            if (ary1 && ary1.length == 0) {
                ary1 = arguments[i].concat(arguments[i + 1]);
            } else {
                ary1 = ary1.concat(arguments[i + 1]);
            }
        }
    }
    ary = ary1.distinct();
    ary1 = null;
    return ary;
}
//complement:在数组的类方法中添加获取数组补集的方法
Array.complement = function() {
    var ary = [];
    if (arguments.length >= 2) {
        ary = Array.difference(Array.union.apply(this, arguments), Array.intersection.apply(this, arguments));
    }
    return ary;
}

var a = [1, 2, 3, 4, 5];
var b = [3, 4, 5, 6, 7];
var c = [5, 6, 7, 8, 9];
var d = [3, 4, 5, 6, 8, 9];
console.log(Array.intersection(a, b, c, d)); //交集
console.log(Array.difference(a, b, c, d)); //差集
console.log(Array.union(a, b, c, d)); //并集
console.log(Array.complement(a, b, c, d)); //补集
console.log(a);
console.log(b);
console.log(c);
console.log(d);