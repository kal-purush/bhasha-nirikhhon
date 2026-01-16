
export type HeaderEntry = [string, string];
export type Headers = HeaderEntry[];

/*
export interface IHeaders {
    [Symbol.iterator](): IterableIterator<HeaderEntry>;
    length: number;
    toArray(): HeaderEntry[];
    getFirst(k: string): string;
    has(k: string): boolean;
    get(k: string): string[];
    add(k: string, v: string): void;
    remove(k: string): this;
}

export class Headers implements IHeaders {
    _h: HeaderEntry[];

    constructor(h?: Iterable<HeaderEntry>) {
        for(let [k, v] of h) {
            this._h.push([k, v]);
        }
    }
    get length() {
        return this._h.length;
    }
    [Symbol.iterator](): IterableIterator<HeaderEntry> {
        return this._h[Symbol.iterator]();
    }
    toArray(): HeaderEntry[] {
        return this._h.map(e => <HeaderEntry> e.slice(0));
    }
    get(key: string): string[] {
        return this._h.filter(e => e[0] === key).map(e => e[1]);
    }
    has(key: string): boolean {
        return this._h.some(e => e[0] === key);
    }
    getFirst(key: string): string {
        let r = this.get(key);
        return r.length === 0 ? null : r[0];
    }
    add(key: string, value: string): void {
        this._h.push([key, value]);
    }
    remove(key: string): Headers {
        this._h = this._h.filter();
    }
}
*/

export class Header {
    static has(h: Headers, k: string): boolean {
        return h.some(e => e[0] === k);
    }
    static get(h: Headers, k: string): string[] {
        return h.filter(e => e[0] === k).map(e => e[1]);
    }
    static getFirst(h: Headers, k: string): string {
        let r = Header.get(h, k);
        return r.length === 0 ? null : r[0];
    }
    static add(h: Headers, k: string, v: string): Headers {
        return h; // TODO!
    }
    static removeAll(k: string): Headers {
        // return h.filter()
        return null;
    }
    static clone(h: Headers): Headers {
        return null;
    }
}
