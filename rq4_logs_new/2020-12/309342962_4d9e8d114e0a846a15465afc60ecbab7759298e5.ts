import { createResult } from './create-result';
import type { Nullable, TypeCheckFunction } from './types';

export function processString<K extends string>(
  key: string,
  keys: K[],
  isKey: TypeCheckFunction<K>,
  special: Nullable<Record<string, K[]>>,
  input?: Record<K, boolean>,
): Record<K, boolean> | void {

  const specialKeys: Nullable<K[]> = special && special[key];

  if (specialKeys) {
    const result = input || createResult(keys, false);
    const { length: len } = specialKeys;
    for (let i = 0; i < len; i++) {
      result[specialKeys[i]] = true;
    }
    return result;
  }

  if (isKey(key)) {
    const result = input || createResult(keys, false);
    result[key] = true;
    return result;
  }

}