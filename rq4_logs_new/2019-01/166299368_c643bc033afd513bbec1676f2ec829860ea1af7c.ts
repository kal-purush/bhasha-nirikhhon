import Class from '../models/Class';

export default abstract class InjectorUtil {
    public static getSymbol(item: Function) {
        return Symbol.for(item.name);
    }

    public static isInheritedFrom<T>(base: Function, inherited: Class<T>): boolean {
        let proto = Object.getPrototypeOf(inherited);

        while (proto && proto !== Function.prototype) {
            if (proto === base) {
                return true;
            }

            proto = Object.getPrototypeOf(proto);
        }

        return false;
    }
}