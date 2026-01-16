/**
 * Recursively remove null and undefined properties from
 * a given object
 * @param obj
 */
export const removeEmpty = <T extends {}>(obj: T): Partial<T> =>
  Object.fromEntries(
    Object.entries(obj)
      .filter(([_, v]) => v != null)
      .map(([k, v]) => {
        return typeof v === 'object' && !(v instanceof Array)
          ? [k, removeEmpty(v!)]
          : [k, v];
      }) as unknown[][]
  );

/**
 * Recursively update object properties with new values.
 * Will take new value for arrays if they exist in the update object
 * @param obj
 * @param update
 */
export const updateObject = <T extends {}>(obj: T, update: T): T => {
  const combinedObject = { ...update, ...obj };

  return Object.fromEntries(
    Object.entries(combinedObject).map(([k, v]) => {
      if (typeof v === 'object' && v != null && !(v instanceof Array)) {
        return update.hasOwnProperty(k)
          ? [k, updateObject(v, update[k as keyof T] as Object)]
          : [k, v];
      } else {
        return update.hasOwnProperty(k)
          ? [k, update[k as keyof T] as Object]
          : [k, v];
      }
    }) as unknown[][]
  );
};