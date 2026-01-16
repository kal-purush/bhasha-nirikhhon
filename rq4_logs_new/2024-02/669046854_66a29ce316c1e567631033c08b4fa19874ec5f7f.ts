import { Application } from "express";
import Stripe from "stripe";
import { stripe_api_key, stripe_api_version } from "./stripe-setting";

const stripe = new Stripe(stripe_api_key, {
  apiVersion: stripe_api_version,
});
const calculateOrderAmount = (items: any[]): number => {
  return 1400;
};

export const configureStripe = (app: Application) => {
  app.post("/create-payment-intent", async (req, res) => {
    const { items } = req.body;

    const paymentIntent = await stripe.paymentIntents.create({
      amount: calculateOrderAmount(items),
      currency: "inr",
      automatic_payment_methods: {
        enabled: true,
      },
    });

    //This is the client secret that will be used in the frontend to confirm the payment intent
    res.send({
      clientSecret: paymentIntent.client_secret,
    });
  });
};