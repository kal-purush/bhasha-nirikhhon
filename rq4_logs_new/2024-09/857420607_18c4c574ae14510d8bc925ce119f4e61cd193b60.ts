import faker, { fakerEN } from '@faker-js/faker';

export class UserDetails {
  private title: string;
  private firstName: string;
  private lastName: string;
  private email: string;
  private phoneNumber: string;

  constructor() {
    this.title = fakerEN.person.prefix()
    this.firstName = fakerEN.person.firstName()
    this.lastName = fakerEN.person.lastName()
    this.email = fakerEN.internet.email()
    this.phoneNumber = fakerEN.phone.number()
  }

  userDetails() {
    return {
      title: this.title,
      firstName: this.firstName,
      lastName: this.lastName,
      email: this.email,
      phoneNumber: this.phoneNumber
    }
  }

}
