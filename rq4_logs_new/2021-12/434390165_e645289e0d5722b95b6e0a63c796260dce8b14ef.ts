import type { Exports } from './types';

/**
 * Strips the index signature from `T` and returns the explicitly defined
 * properties.
 *
 * @internal
 * @see https://stackoverflow.com/a/51956054/420747
 */
type RemoveIndex<T> = {
  [K in keyof T as string extends K
    ? never
    : number extends K
    ? never
    : K]: T[K];
};

/**
 * @internal
 */
export type RegistryShape = {
  [ModuleId in string]?: Exports;
};

/**
 * Augment this interface to define strongly-typed modules.
 *
 * @example
 * ```ts
 * declare module 'loader.ts' {
 *   interface Registry {
 *     foo: { bar: string };
 *     qux: { bar: boolean; default: number };
 *   }
 * }
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface Registry extends RegistryShape {}

/**
 * Explicitly defined modules.
 *
 * @internal
 */
export type KnownModules = RemoveIndex<Registry>;