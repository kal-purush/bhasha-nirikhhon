export declare const func: (fn: Function, numArgs?: number) => Function;
export declare const pipe: (...fns: Function[]) => Function;
export declare const compose: (...fns: Function[]) => Function;
export declare const flip: <a, b, c>(fn: (b: b, a: a, ...x: any[]) => c) => (a: a, b: b, ...x: any[]) => c;
export declare const map: Function;
export declare const foldl: Function;
export declare const foldr: Function;
export declare const reverse: <a>([x, ...xs]: a[]) => a[];
export declare const listc: () => string;