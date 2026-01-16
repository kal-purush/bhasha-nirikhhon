import axios from 'axios';

import apiConfig from '../configs/api';
import transformBEOrder from '../utils/transformBEOrder';

import type { Order } from '../types/order.type';

const createOrder = async (
  order: Omit<Order, 'id' | 'paymentStatus' | 'orderedDate'>,
) => {
  const response = await axios.post(
    `${apiConfig.baseUrl}/orders`,
    transformBEOrder(order),
  );

  return response.data;
};

const createGuestOrder = async (
  order: Omit<Order, 'id' | 'paymentStatus' | 'orderedDate'>,
) => {
  const response = await axios.post(
    `${apiConfig.baseUrl}/orders/guest`,
    transformBEOrder(order),
  );

  return response.data;
};

const ordersApi = { createOrder, createGuestOrder };

export default ordersApi;