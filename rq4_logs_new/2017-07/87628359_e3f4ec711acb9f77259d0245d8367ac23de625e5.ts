import {Morphism} from './$types';

export type DeepPartial<T> = {
  [K in keyof T]?: DeepPartial<T[K]>;
};

// from https://github.com/DefinitelyTyped/DefinitelyTyped/blob/f8fb828/types/ramda/index.d.ts#L64
export type Evolver<T> =
  | Morphism<T, T>
  | { [K in keyof T]?: Evolver<T[K]> };

// from https://github.com/Microsoft/TypeScript/issues/12215#issuecomment-311923766
export type Diff<T extends string, U extends string> = ({ [P in T]: P } & { [P in U]: never } & { [x: string]: never })[T];
export type Omit<T, K extends keyof T> = Pick<T, Diff<keyof T, K>>;