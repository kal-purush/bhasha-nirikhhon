import {ProductType} from '../types/ProductType';
import api from './api';

export const getAllProducts = async (): Promise<ProductType[]> => {
  const response = await api.get('/products');
  const {data} = response;
  return data;
};