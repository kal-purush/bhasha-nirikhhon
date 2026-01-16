import faker from "faker/locale/en";

import Address from "./address";
import Contact from "./contact";

export default class ContactStore {
  public fetch(): Contact[] {
    return generateContacts();
  }
}

function generateContacts(): Contact[] {
  return [
    generateContact(),
    generateContact(),
    generateContact(),
    generateContact(),
    generateContact(),
  ];
}

function generateContact(): Contact {
  const address = new Address(
    faker.address.streetAddress(), faker.address.secondaryAddress(),
    faker.address.city(), faker.address.state(), faker.address.zipCode());
  return new Contact(faker.name.firstName(), faker.name.lastName(), address);
}