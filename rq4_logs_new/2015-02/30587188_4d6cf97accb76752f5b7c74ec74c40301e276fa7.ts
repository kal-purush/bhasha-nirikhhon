module GameFlow {

    function cloneArray(val: Array<any>): Array<any> {
        return val.slice(0);
    }

    interface AnyObject {
        [idx: string]: any;
    }

    function cloneObject(val: AnyObject): AnyObject {
        var cloned:typeof val = {};
        Object.keys(val).forEach((key:string) => {
            cloned[key] = val[key];
        });
        return cloned;
    }

    function cloneDate(val: Date): Date {
        return new Date(val.getTime());
    }

    export function clone(val: any): any {
        if (typeof val === 'object' && val !== null) {
            if (Array.isArray(val)) {
                return cloneArray(<any[]> val);
            } else if (val instanceof Date) {
                return cloneDate(val);
            } else {
                return cloneObject(val);
            }
        } else {
            return val;
        }
    }

}