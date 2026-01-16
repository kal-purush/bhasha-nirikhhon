import { Request, Response } from "express";
import { stripe } from "../payments/stripe/stripe";
import { endpointSecret } from "./stripe-wh-setting";
import Order from "../src/model/order/order.model";

export const handleStripeWebhook = async (req: Request, res: Response) => {
  const sig: any = req.headers["stripe-signature"];

  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    res.status(400).send(`Webhook Error: ${(err as Error).message}`);
    return;
  }

  // Handle the event
  switch (event.type) {
    case "payment_intent.succeeded":
      const paymentIntentSucceeded = event.data.object;
      // TODO: Update the order payment status
      const order = await Order.findById(
        paymentIntentSucceeded.metadata.orderId,
      );
      // order.paymentStatus = 'received';
      // await order.save();
      // Then define and call a function to handle the event payment_intent.succeeded
      break;
    // ... handle other event types
    default:
  }

  // Return a response to acknowledge receipt of the event
  res.send();
};