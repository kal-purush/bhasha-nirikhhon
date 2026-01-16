import useSWR from 'swr';
import axios from 'axios';

import apiConfig from '../configs/api';
import transformProduct from '../utils/transformProduct';

import type { Product } from '../types/product.type';

const useProduct = (id: string) => {
  const {
    isLoading,
    data: product,
    error,
  } = useSWR(`/products/${id}`, async (url: string) => {
    const response = await axios.get<Product>(`${apiConfig.baseUrl}${url}`);

    return transformProduct(response.data);
  });

  return {
    isLoading,
    product,
    error,
  };
};

export default useProduct;