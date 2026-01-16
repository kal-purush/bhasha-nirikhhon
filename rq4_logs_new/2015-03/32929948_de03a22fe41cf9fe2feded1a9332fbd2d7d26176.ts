import domSelector = require('./interfaces');

var toStr:Function = Object.prototype.toString;
var maxSafeInteger:number = Math.pow(2, 53) - 1;
var hasOwnProperty = Object.prototype.hasOwnProperty;

function isCallable(fn:any):boolean {
    return typeof fn === 'function' || toStr.call(fn) === '[object Function]';
}

function toInteger(value:any):number {
    var num = Number(value);
    if (isNaN(num)) { return 0; }
    if (num === 0 || !isFinite(num)) { return num; }
    return (num > 0 ? 1 : -1) * Math.floor(Math.abs(num));
}

function toLength(value:any):number {
    var len = toInteger(value);
    return Math.min(Math.max(len, 0), maxSafeInteger);
}

function getMaxIndexProperty(object:Object):number {
    var maxIndex:number = -1,
        isValidProperty:boolean;

    for (var prop in object) {
        isValidProperty = (String(toInteger(prop)) === prop && toInteger(prop) !== maxSafeInteger &&
            hasOwnProperty.call(object, prop));
        if (isValidProperty && prop > maxIndex) {
            maxIndex = prop;
        }
    }

    return maxIndex;
}

class ExtensionArray<T> {
    constructor() {
        Array.apply(this, arguments);
        return new Array();
    }
    pop():any { }
    push(val:T):number { return 0; }
    splice(start:number, deleteCount:number, ...items:any[]):any { }
    length: number;
}

ExtensionArray['prototype'] = new Array();

class NodeArray extends ExtensionArray<Node|HTMLElement> implements domSelector.INodeArray {
    [index:number]: Node|HTMLElement;
    private _length:number = 0;
    private _isMutating:boolean = false;
    get length():number {
        var maxIndexProperty:number = +getMaxIndexProperty(this);
        return Math.max(this._length, maxIndexProperty + 1);
    }
    set length(value: number) {
        var constrainedValue:number = toLength(value),
            currentLength:number = this.length;
        if (constrainedValue !== +value) {
            throw new RangeError();
        }
        if (!this._isMutating && constrainedValue < currentLength) {
            this._isMutating = true;
            this.splice(constrainedValue, currentLength - constrainedValue);
            this._isMutating = false;
        }
        this._length = constrainedValue;
    }

    static from(arrayLike:any, mapFn?:Function, thisArg?:Object):NodeArray {
        var C:Function = this,
            items = Object(arrayLike);

        if (arrayLike === null) {
            throw new TypeError('NodeArray.from requires an array-like object - not null or undefined');
        }

        var T:Object;
        if (typeof mapFn !== 'undefined') {
            if (!isCallable(mapFn)) {
                throw new TypeError('NodeArray.from: when provided, the second argument must be a function');
            }
            if (arguments.length > 2) {
                T = arguments[2];
            }
        }

        var len = toLength(items.length),
            NA:NodeArray = new NodeArray(len),
            k:number = 0,
            kValue:any;

        while (k < len) {
            kValue = items[k];
            if (mapFn) {
                NA[k] = typeof T === 'undefined' ? mapFn(kValue, k) : mapFn.call(T, kValue, k);
            }
            else {
                NA[k] = kValue;
            }
            k += 1;
        }
        NA.length = len;
        return NA;
    }

    constructor();
    constructor(length:number);
    constructor(...items:any[]) {
        super();
        var length:number = 0;
        if (items.length === 1 && typeof items[0] === 'number') {
            length = items[0];
        }
        else if (items.length > 1) {
            for (var i = 0; i < items.length; i++) {
                this[i] = items[i];
            }
            length = items.length;
        }
    }
}

export = NodeArray;