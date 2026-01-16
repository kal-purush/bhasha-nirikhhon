import { client } from './helpers/httpClient';
import { Order } from './types/Categories';
import { Product } from './types/Product';
import { ProductDetails } from './types/ProductDetails';
import { Address, Login } from './types/User';

export const getProducts = () => {
  return client.get<Product[]>('products');
};


export const getProductDetails = (productId: string) => {
  return client.get<ProductDetails>(`products/${productId}`);
};

export const toggleWhishilist = (productId: string) => {
  return client.get<ProductDetails>(`products/${productId}/wishlist`);
};


export const sendOrder = (orderItems: Order[]) => {
  return client.post<Order[]>('orders', orderItems);
};

export const verifyToken = (user: Login) => {
  return client.post<Login>('token/verify', user);
};

export const addAddress = (address: Address) => {
  return client.post<Address>('me/address', address);
};