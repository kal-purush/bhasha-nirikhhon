import { NextResponse } from 'next/server';
import Stripe from 'stripe';
import { stripe } from '@rocket-house-productions/integration';

//add swagger docs
/**
 * @swagger
 * /api/webhook:
 *   post:
 *     tags:
 *       - Webhook
 *     summary: Stripe webhook
 *     description: Stripe webhook
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               stripe:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: string
 *                     required: true
 *                   hash:
 *                     type: string
 *                     required: true
 *     responses:
 *       200:
 *         description: Successful operation
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 status:
 *                   type: string
 */
export async function POST(req: Request, res: Response) {
  let event: Stripe.Event;

  try {
    event = stripe.webhooks.constructEvent(
      await (await req.blob()).text(),
      req.headers.get('stripe-signature') as string,
      process.env.STRIPE_WEBHOOK_SECRET as string,
    );
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : 'Unknown error';
    // On error, log and return the error message.
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    if (err! instanceof Error) console.log(err);
    console.log(`❌ Error message: ${errorMessage}`);
    return NextResponse.json({ message: `Webhook Error: ${errorMessage}` }, { status: 400 });
  }

  // Successfully constructed event.
  console.log('✅ Success:', event.id);

  const permittedEvents: string[] = [
    'invoice.paid',
    'charge.succeeded',
    'charge.failed',
    'checkout.session.expired',
    'checkout.session.async_payment_succeeded',
    'checkout.session.completed',
    'payment_intent.succeeded',
    'payment_intent.payment_failed',
  ];

  if (permittedEvents.includes(event.type)) {
    let data;

    try {
      switch (event.type) {
        case 'checkout.session.completed':
          data = event.data.object as Stripe.Checkout.Session;
          console.log(`💰 CheckoutSession status: ${data.payment_status}`);
          break;
        case 'checkout.session.async_payment_failed':
          data = event.data.object as Stripe.Checkout.Session;
          console.log(`❌ CheckoutSession status: ${data.payment_status}`);
          break;
        case 'checkout.session.async_payment_succeeded':
          data = event.data.object as Stripe.Checkout.Session;
          console.log(`💰 CheckoutSession status: ${data.payment_status}`);
          break;
        case 'checkout.session.expired':
          data = event.data.object as Stripe.Checkout.Session;
          //await cancelBooking(data.metadata.id);
          console.log(`❌ CheckoutSession status: ${data.payment_status}`);
          break;
        case 'payment_intent.payment_failed':
          data = event.data.object as Stripe.PaymentIntent;
          //await cancelBooking(data.metadata.id);
          console.log(`❌ Payment failed: ${data.last_payment_error?.message}`);
          break;
        case 'payment_intent.succeeded':
          data = event.data.object as Stripe.PaymentIntent;
          console.log(`💰 PaymentIntent status: ${data.status}`);
          break;
        case 'charge.failed':
          data = event.data.object as Stripe.Charge;
          //await cancelBooking(data.metadata.id);
          console.log(`❌ Charge status: ${data.status}`);
          break;
        case 'charge.succeeded':
          data = event.data.object as Stripe.Charge;
          console.log(`💰 Charge status: ${data.metadata.invoice_id}`);
          //TODO update user status here
          console.log(`💰 Charge status: ${data.status}`);
          break;
        case 'invoice.paid':
          data = event.data.object as Stripe.Invoice;
          console.log(`💰 Invoice status: ${data.id}`);
          //const charge = data.charge as Stripe.Charge;
          //TODO update user status here to paid
          console.log(`💰 Invoice status: ${data.status}`);
          break;
        default:
          throw new Error(`Unhandled event: ${event.type}`);
      }
    } catch (error) {
      console.log(error);
      return NextResponse.json({ message: 'Webhook handler failed' }, { status: 500 });
    }
  }

  return NextResponse.json({ message: 'Received' }, { status: 200 });
}