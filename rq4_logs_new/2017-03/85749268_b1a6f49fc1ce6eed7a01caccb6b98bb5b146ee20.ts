


// export interface compose(
//    ...funcs: Function[]
// ): (...args: any[]) =>  any[];

  export function compose<A, B, R>(
    f1: (a: B) => R, f2: (a: A) => B,
    ...funcs: Function[]
  ): (...args: any[]) => R;


// export function compose(x: Function, y: Function, ...funcs: Function[]): (...args: any[]) => any[];

// declare namespace Redux { //TODO: better method?


//   export interface compose<A, B, R>(
//     f1: (a: B) => R, f2: (a: A) => B,
//     ...funcs: Function[]
//   ): (...args: any[]) => R;


// }