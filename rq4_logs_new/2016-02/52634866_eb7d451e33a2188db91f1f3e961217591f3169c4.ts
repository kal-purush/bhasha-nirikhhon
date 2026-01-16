//Shamelessly stolen from https://github.com/lodash/lodash/blob/4.3.0/lodash.js
class TuilConsts {
    static rePropName = /[^.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|([""])((?:(?!\2)[^\\]|\\.)*?)\2)\]/g;
    static INFINITY = 1 / 0;
    static reEscapeChar = /\\(\\)?/g;
    static reIsDeepProp = /\.|\[(?:[^[\]]*|([""])(?:(?!\1)[^\\]|\\.)*?\1)\]/;
    static reIsPlainProp = /^\w*$/;
    static MAX_SAFE_INTEGER = 9007199254740991;
    static MAX_INTEGER = 1.7976931348623157e+308;
    static NAN = 0 / 0;
}

export class Tuil {
    private static toString(value) {
        // Exit early for strings to avoid a performance hit in some environments.
        if (typeof value == "string") {
            return value;
        }
        if (value == null) {
            return "";
        }
        var result = (value + "");
        return (result == "0" && (1 / value) == -TuilConsts.INFINITY) ? "-0" : result;
    }

    private static stringToPath(str:string) {
        var result = [];
        Tuil.toString(str).replace(TuilConsts.rePropName, function(match, num, quote, str) {
            result.push(quote ? str.replace(TuilConsts.reEscapeChar, "$1") : (num || match));
        });
        return result;
    }

    private static baseToPath(value) {
        return Array.isArray(value) ? value : Tuil.stringToPath(value);
    }

    private static isKey(value, object) {
        if (typeof value == "number") {
            return true;
        }
        return !Array.isArray(value) &&
            (TuilConsts.reIsPlainProp.test(value) || !TuilConsts.reIsDeepProp.test(value) ||
                (object != null && value in Object(object)));
    }

    private static baseGet(object, path) {
        path = Tuil.isKey(path, object) ? [path + ""] : Tuil.baseToPath(path); 
        var index = 0,
            length = path.length;

        while (object != null && index < length) {
            object = object[path[index++]];
        }
        return (index && index == length) ? object : undefined;
    }

    public static get(object, path, defaultValue) {
        var result = object == null ? undefined : Tuil.baseGet(object, path);
        return result === undefined ? defaultValue : result;
    }
}