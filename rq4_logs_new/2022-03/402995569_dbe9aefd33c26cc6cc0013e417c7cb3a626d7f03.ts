import { Dictionary, fromPairs, sortBy, toPairs } from "lodash";

// https://stackoverflow.com/questions/32349838/lodash-sorting-object-by-values-without-losing-the-key
export const sortObjectByValues = (obj: Dictionary<unknown>): Dictionary<unknown> => {
  return fromPairs(sortBy(toPairs(obj), 1));
};