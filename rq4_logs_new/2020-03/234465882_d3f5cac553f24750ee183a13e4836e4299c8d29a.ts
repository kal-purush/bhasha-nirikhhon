const faker = require('faker')

// Stripe testing credit cards
const creditCards = [
  '4242424242424242',
  '4000056655665556',
  '5555555555554444'
]

const generateBillingProfiles = (amount: number) => {
  const billingProfiles = []

  for (let i = 0; i < amount; i++) {
    const firstName = faker.firstName()
    const lastName = faker.lastName()
    const userInfo = {
      firstName,
      lastName,
      address1: faker.address.streetAddress(),
      address2: faker.address.secondaryAddress(),
      city: faker.address.city(),
      state: faker.address.state(),
      country: faker.address.country(),
      zipCode: faker.address.zipCode(),
      phone: faker.phone.phoneNumber(),
    }
    const paymentInfo = {
      nameOnCard: `${firstName} ${lastName}`,
      cardNumber: creditCards[Math.floor(Math.random() * Math.floor(3))],
      expirationMonth: '12',
      expirationYear: '24',
      securityCode: '342',
      email: faker.internet.email(),
      profileName: faker.internet.userInfo(),
    }
    const billingProfile = {
      billing: userInfo,
      shipping: userInfo,
      payment: paymentInfo
    }
    billingProfiles.push(billingProfile)
  }

  return billingProfiles
}

export default generateBillingProfiles