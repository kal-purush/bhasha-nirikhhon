declare module "nedb-isotropy" {
    declare type CountOptionsType = {
        limit?: number;
        skip?: number
    };

    declare class Cursor {
        exec(cb: (err?: Error, result: Array<Object>) => void) : void;
        limit(n: number) : void;
        skip(n: number) : void;
        sort(fields: Object) : void;
    }

    declare class Datastore {
        count(query: Object, cb: (err?: Error, result: number) => void) : void;
        insert(docs: Array<Object>, cb: (err?: Error, result: Array<number>) => void) : void;
        find(query: Object, fields: Object) : Cursor;
        remove(filter: Object, options: { multi: boolean }, cb: (err?: Error, result: number) => void) : void;
        update(selector: Object, update: Object, options: { multi: boolean }, cb: (err?: Error, result: number) => void) : void;
    }
}