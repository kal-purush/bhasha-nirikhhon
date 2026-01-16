import { ProductType } from '../types/ProductType';
import { ShuffledArray } from '../utils/shuffledArray';
import { fetchProducts } from '../utils/mockApi';

export const getAllProducts = async (): Promise<ProductType[]> => {
  return fetchProducts();
};

export const getProductsByCategory = async (category: string) => {
  const products = await fetchProducts();

  return products.filter((product: ProductType) => product.category === category);
};

export const getHotPriceProducts = async () => {
  const response = await fetchProducts();

  return response.sort((a: ProductType, b: ProductType) => {
    return b.fullPrice - b.price - (a.fullPrice - a.price);
  });
};

export const getNewProducts = async () => {
  const response = await fetchProducts();
  const latestYear = response.reduce(
    (acc: number, product: ProductType) => Math.max(acc, product.year),
    0,
  );

  return response
    .filter((product: ProductType) => product.year === latestYear)
    .sort((a: ProductType, b: ProductType) => b.fullPrice - a.fullPrice);
};

export const getSuggestedProducts = async () => {
  const products = await fetchProducts();
  const suggestedProducts = ShuffledArray(products);

  return suggestedProducts;
};