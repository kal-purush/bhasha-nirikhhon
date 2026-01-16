import { createObjectResolver } from '../resolve-object';
import type { Nullable, TypeCheckFunction } from '../types';

/** @deprecated */
export function resolveObject<K extends string, V, D = V>(
  value: unknown,
  keys: K[],
  isKey: TypeCheckFunction<K>,
  special: Nullable<Record<string, K[]>>,
  isValidValue: TypeCheckFunction<V>,
  defaultValue: D,
): Record<K, V | D> | void {
  return createObjectResolver(
    keys,
    isValidValue,
    defaultValue,
    isKey,
    special,
  )(value);
}