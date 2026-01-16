function pair<T1, T2>(x: T1, y: T2) {
    return <[T1, T2]>[x, y];
}

function triple<T1, T2, T3>(x: T1, y: T2, z: T3) {
    return <[T1, T2, T3]>[x, y, z];
}

function flatten<T>(arrs: T[][]): T[] { return [].concat.apply([], arrs); }

function first<T1, T2, T3>(tuple: [T1, T2] | [T1, T2, T3]) {
    return tuple[0];
}

function second<T1, T2, T3>(tuple: [T1, T2] | [T1, T2, T3]) {
    return tuple[1];
}

function third<T1, T2, T3>(tuple: [T1, T2, T3]) {
    return tuple[2];
}

function range(first: number, last: number): number[] {
    var arr = new Array(last - first + 1);
    for (var i = first; i <= last; ++i) {
        arr[i - first] = i;
    }
    return arr;
}

function zipWith<T1, T2, T3>(arr1: T1[], arr2: T2[], f: (arg1: T1, arg2: T2) => T3): T3[] {
    var res = [];
    var minLen = Math.min(arr1.length, arr2.length);
    for (var i = 0; i < minLen; ++i) {
        res.push(f(arr1[i], arr2[i]));
    }
    return res;
}

function comp<T1, T2, T3>(f1: (a: T2) => T3, f2: (b: T1) => T2): (c: T1) => T3 {
    return (x) => f1(f2(x));
}

function extend<T, U>(first: T, second: U): T & U {
    let result = <T & U>{};
    for (let id in first) {
        result[id] = first[id];
    }
    for (let id in second) {
        if (!result.hasOwnProperty(id)) {
            result[id] = second[id];
        }
    }
    return result;
}

function merge<T, U>(base: T, change: U): T {
    let result = <T>{};
    for (let id in base) {
        result[id] = base[id];
    }
    for (let id in change) {
        result[id] = change[id];
    }
    return result;
}