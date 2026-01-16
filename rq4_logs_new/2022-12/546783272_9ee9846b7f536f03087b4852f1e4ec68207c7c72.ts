import { NextApiHandler } from "next";
import { Stripe } from "stripe";
import { apolloClient } from "../../graphql/apolloClient";
import {
  GetProductBySlugDocument,
  GetProductBySlugQuery,
  GetProductBySlugQueryVariables,
} from "../../generated/graphql";

const checkoutHandler: NextApiHandler = async (req, res) => {
  const stripeKey = process.env.STRIPE_SECRET_KEY;

  if (!stripeKey) {
    res.status(500).json({ message: `Missing STRIPE_SECRET_KEY` });
    return;
  }

  const body = req.body as {
    slug: string;
    count: number;
  }[];

  const products = await Promise.all(
    body.map(async (cartItem) => {
      const product = await apolloClient.query<
        GetProductBySlugQuery,
        GetProductBySlugQueryVariables
      >({
        query: GetProductBySlugDocument,
        variables: {
          slug: cartItem.slug,
        },
      });

      return {
        product,
        count: cartItem.count,
      };
    })
  );

  const stripe = new Stripe(stripeKey, { apiVersion: "2022-11-15" });

  const stripeCheckoutSession = await stripe.checkout.sessions.create({
    mode: "payment",
    locale: "pl",
    payment_method_types: ["p24", "card"],
    success_url:
      "http://localhost:3000/checkout/success?session_id={CHECKOUT_SESSION_ID}",
    cancel_url: "http://localhost:3000/checkout/cancel",
    line_items: products.map((product) => {
      return {
        adjustable_quantity: {
          enabled: true,
          minimum: 0,
          maximum: 99,
        },
        price_data: {
          currency: "PLN",
          unit_amount: product.product.data.product!.price,
          product_data: {
            name: product.product.data.product!.name,
            images: product.product.data.product!.images.map((i) => i.url),
            metadata: { slug: product.product.data.product!.slug },
          },
        },
        quantity: product.count,
      };
    }),
  });

  res.status(201).json({ session: stripeCheckoutSession });
};

export default checkoutHandler;