import { stripe } from './providers';
import { extend } from './util';

export function createCharge(amount, token, email): Promise<number> {
  return new Promise((resolve, reject) => {
    // charge customer
    stripe.charges.create({
      amount: amount,
      currency: "eur",
      source: token, // obtained with Stripe.js
      description: `Credit charge for ${email}`
    }, (err, charge) => {
      if (err) {
        console.error(err);
        return reject(extend(new Error('Error while charging credit card'), { status: 500 }));
      }

      resolve(charge.amount);
    });
  });
}

export function getCustomer(id: string): Promise<any> {
  return new Promise((resolve, reject) => {
    stripe.customers.retrieve(
      id,
      (err, customer) => {
        if (err) {
          console.error(err);
          return reject(extend(new Error('Error while receiving user from payment service'), { status: 500 }));
        }

        resolve(customer);
      }
    );
  });
}

export function updateCustomer(id: string, diff: any): Promise<{}> {
  return new Promise((resolve, reject) => {
    stripe.customers.update(
      id,
      diff,
      (err, _) => {
        if (err) {
          console.error(err);
          return reject(extend(new Error('Error while updating user at payment service'), { status: 500 }));
        }

        resolve();
      });
  });
}