import { describe, expect, it } from 'vitest';
import { faker } from '@faker-js/faker/locale/pt_BR';

import renameObjectToCamelCase from '.';

describe('renameObjectToCamelCase', () => {
  it('Must rename all object keys to camelCase format', () => {
    const fakerObject = {
      first_name: faker.name.firstName(),
      full_name: faker.name.fullName(),
      info_payment: {
        credit_card_number: faker.finance.creditCardNumber(),
        credit_card_issuer: faker.finance.creditCardIssuer(),
      },
      my_addresses: [{ street_name: faker.address.streetAddress() }, { country: faker.address.country() }],
    };

    const res = renameObjectToCamelCase({ obj: fakerObject });

    expect(res).toMatchObject({
      firstName: fakerObject.first_name,
      fullName: fakerObject.full_name,
      infoPayment: {
        creditCardNumber: fakerObject.info_payment.credit_card_number,
        creditCardIssuer: fakerObject.info_payment.credit_card_issuer,
      },
      myAddresses: [
        { streetName: fakerObject.my_addresses[0].street_name },
        { country: fakerObject.my_addresses[1].country },
      ],
    });
  });
});