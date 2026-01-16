export function flattenObj(
  obj: Record<string, unknown>,
  parentKey = '',
  separator = '.'
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      const value = obj[key];
      const newKey = parentKey ? `${parentKey}${separator}${key}` : key;

      if (
        typeof value === 'object' &&
        value !== null &&
        !Array.isArray(value)
      ) {
        Object.assign(
          result,
          flattenObj(value as Record<string, unknown>, newKey, separator)
        );
      } else {
        result[newKey] = value;
      }
    }
  }

  return result;
}

export function flattenArray(
  arr: Record<string, unknown>[],
  separator = '.'
): Record<string, unknown>[] {
  return arr.map(obj => flattenObj(obj, '', separator));
}