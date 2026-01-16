// ============= Test Cases =============

import type { Equal, Expect } from './test-utils'

type Arr = [1, 2, 3, 4, 5]

type cases = [
  // basic
  Expect<Equal<Slice<Arr, 0, 1>, [1]>>,
  Expect<Equal<Slice<Arr, 0, 0>, []>>,
  Expect<Equal<Slice<Arr, 2, 4>, [3, 4]>>,

  // optional args
  Expect<Equal<Slice<[]>, []>>,
  Expect<Equal<Slice<Arr>, Arr>>,
  Expect<Equal<Slice<Arr, 0>, Arr>>,
  Expect<Equal<Slice<Arr, 2>, [3, 4, 5]>>,

  // negative index
  Expect<Equal<Slice<Arr, 0, -1>, [1, 2, 3, 4]>>,
  Expect<Equal<Slice<Arr, -3, -1>, [3, 4]>>,

  // invalid
  Expect<Equal<Slice<Arr, 10>, []>>,
  Expect<Equal<Slice<Arr, 1, 0>, []>>,
  Expect<Equal<Slice<Arr, 10, 20>, []>>
]

// ============= Your Code Here =============

declare const UNSET: unique symbol

type BuildTuple<
  N extends number,
  Res extends readonly unknown[] = []
> = Res extends { readonly length: N } ? Res : BuildTuple<N, [...Res, unknown]>

type Add<N0 extends number, N1 extends number> = [
  ...BuildTuple<N0>,
  ...BuildTuple<N1>
]['length']

type Subtract<
  N0 extends number,
  N1 extends number,
  Res extends readonly unknown[] = []
> = BuildTuple<N1> extends readonly []
  ? [...Res, unknown]['length']
  : [BuildTuple<N0>, BuildTuple<N1>] extends readonly [
      [unknown, ...infer R0],
      [unknown, ...infer R1]
    ]
  ? Subtract<R0['length'], R1['length'], [...Res, unknown]>
  : never

type Slice<
  Arr extends readonly unknown[],
  Start extends number = 0,
  End extends number = Arr['length'],
  Res extends readonly unknown[] = [],
  Head extends readonly unknown[] = []
> = Arr extends readonly []
  ? []
  : Arr extends readonly [infer H, ...infer R]
  ? Head['length'] extends Start
    ? [...Head, ...Res]['length'] extends End
      ? Res
      : Slice<
          R,
          Head['length'],
          End,
          [...Res, H],
          [...Head, ...Res, unknown]['length'] extends End
            ? [...Head, unknown]
            : Head
        >
    : Slice<R, Start, End, Res, [...Head, H]>
  : Res

type c = Subtract<5, 4>