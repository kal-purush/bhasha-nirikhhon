'use server';
import { stripe, stripeCheckout } from './stripe';
import { redirect } from 'next/navigation';
import { auth } from '@clerk/nextjs/server';

export const stripeCheckoutAction = async (data: FormData) => {
  const productId = data.get('productId');

  console.log('stripeCheckoutAction', productId);
  console.log('data', data);

  if (typeof productId !== 'string' || !productId) {
    throw new Error('Invalid product ID');
  }

  const { userId, sessionClaims } = auth();

  if (!userId) {
    return null;
  }

  const checkoutSession = await stripeCheckout(productId, userId, sessionClaims?.email as string);

  if (!checkoutSession?.url) {
    throw new Error('Invalid checkout session url');
  }

  redirect(checkoutSession.url);
};

export default stripeCheckoutAction;