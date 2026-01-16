const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY)
export default async function handler(req: any, res: any) {
  if (req.method === 'POST') {
    const { plan, booking } = JSON.parse(req.body)
    try {
      const session = await stripe.checkout.sessions.create({
        line_items: [
          {
            price_data: {
              unit_amount: plan.price,
              product_data: {
                name: plan.name,
                description: plan.description,
              },
              currency: 'inr',
            },
            quantity: 1,
          },
        ],
        metadata: booking,
        mode: 'payment',
        success_url: `${req.headers.origin}/result?session_id={CHECKOUT_SESSION_ID}`,
        cancel_url: `${req.headers.origin}/`,
      })
      res.status(200).json(session)
    } catch (err: any) {
      console.error(err.message)
      res.status(err.statusCode || 500).json(err.message)
    }
  } else {
    res.setHeader('Allow', 'POST')
    res.status(405).end('Method Not Allowed')
  }
}