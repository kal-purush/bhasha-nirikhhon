export default class ValidationUtil {
    static isEmptyStr(str) {
        return (!str || 0 === str.length || str === undefined || str === null || str === 'null');
    }

    static isEmptyStrArr(x, y, ...z) {
        for (let index = 0; index < z.length; index++) {
            const element = z[index];
            return this.isEmptyStr(element);
        }
    }

    static isNaN(str) {
        return isNaN(parseInt(str));
    }

    static isNaNs(x, y, ...z) {
        for (let index = 0; index < z.length; index++) {
            const element = z[index];
            return this.isNaN(element);
        }
    }

    static isObjectEmpty(obj) {
        return Object.keys(obj).length === 0;
    }

    static isCollectionEmpty(array) {
        return (array === undefined || array.length === 0);
    }
}