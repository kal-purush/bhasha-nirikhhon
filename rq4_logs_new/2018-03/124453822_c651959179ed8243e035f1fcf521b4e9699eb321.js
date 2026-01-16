import stripeApi from 'stripe'
const stripe = stripeApi(process.env.SECRET_KEY)
import User from '../../models/User.js'

module.exports = app => {
  app.post('/api/stripe', (req, res) => {
    // get jwt token
    const userToken = req.headers['authorization'].split(' ')[1]

    // find user based on jwt token
    // User.findOne({ verifiedToken: userToken }).then(async user => {
    User.findOne({ email: req.body.email }).then(async user => {
      if (user.stripe.length > 0) {
        // attempt to add card information to stripe customer
        try {
          console.log(user.stripe)
          const source = await stripe.customers.update(user.stripe, {
            source: req.body.id
          })
        } catch (e) {
          console.log(`customer update error: ${e}`)
        }

        // attempt to charge stripe customer with newly added card
        try {
          const charge = await stripe.charges.create({
            amount: 99,
            currency: 'usd',
            description: '0.99¢ for account',
            customer: user.stripe
          })

          console.log(`charge successful.`)
        } catch (e) {
          console.log(`customer charge error: ${e}`)
        }
      } else {
        console.log('User was not found. Creating new stripe customer.')
        const customer = await stripe.customers.create({
          email: user.email,
          source: req.body.id
        })
      }
    })
  })
}