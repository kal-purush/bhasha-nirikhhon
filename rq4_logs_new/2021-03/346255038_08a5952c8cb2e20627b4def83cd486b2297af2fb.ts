import type { Price, PriceObject } from "src/interfaces/price";

export const emptyPriceObject = (): PriceObject => {
  return {
    key: "",
    ...emptyPrice(),
  };
};

export const emptyPrice = (): Price => {
  return {
    category: undefined,
    amount: 0,
    timestamp: -1,
  };
};

export const ensurePrice = (p?: Partial<Price>): Price => {
  return {
    category: p?.category ?? "-",
    amount: p?.amount ?? 0,
    timestamp: p?.timestamp ?? +new Date(),
  };
};

export const convertPrice = (key: string, p?: Partial<Price>): PriceObject => {
  return {
    key,
    ...ensurePrice(p),
  };
};

export const convertMap = (map: Record<string, Partial<Price>>): PriceObject[] => {
  return Object.keys(map).reduce((result, key) => {
    const price = map[key];

    return result.concat(convertPrice(key, price));
  }, []);
};