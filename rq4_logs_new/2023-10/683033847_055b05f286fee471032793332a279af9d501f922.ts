import { useMutation } from '@tanstack/react-query';
import axios from 'axios';

import { Routes } from '@/utils';
import getStripe from '@/utils/stripe';

const createCheckout = async (price: string) => {
  const stripe = await getStripe();
  const body = {
    price,
  };
  const checkoutSession = await axios.post(Routes.API.CHECKOUT, body);
  const result = await stripe.redirectToCheckout({
    sessionId: checkoutSession.data.id,
  });
};

export const useCreateCheckout = () => {
  const mutation = useMutation({
    mutationFn: (price: string) => createCheckout(price),
  });
  return mutation;
};