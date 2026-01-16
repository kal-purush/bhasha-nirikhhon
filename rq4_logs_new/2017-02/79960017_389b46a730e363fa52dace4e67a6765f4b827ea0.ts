import { Person } from './person.model';

describe('Person', () => {
  let person_1: Person;
  let person_2: Person;
  let address: {};

  describe('instance created with full details', () => {
    beforeEach(() => {
      address = {
        'street': "123 North Kings Drive",
        'city': 'Myrtle Beach',
        'state': 'SC',
        'zip': '29577'
      };
      person_1 = new Person({ 'id': 9, 'name': 'Bob Smith', 'phone': '843-555-1234', 'address': address })
    });

    it('has id', () => {
      expect(person_1.id).toBe(9);
    });

    it('has name', () => {
      expect(person_1.name).toBe('Bob Smith');
    });

    it('has phone number', () => {
      expect(person_1.phone).toBe('843-555-1234');
    });

    it('has address', () => {
      expect(person_1.address).toBe(address);
    })
  })

  describe('instance created with no details', () => {
    beforeEach(() => {
      person_2 = new Person();
    });

    it('has null as id', () => {
      expect(person_2.id).toBe(null);
    });

    it('has empty name field', () => {
      expect(person_2.name).toBe('');
    });

    it('has empty phone number field', () => {
      expect(person_2.phone).toBe('');
    });

    it('has empty address field', () => {
      expect(person_2.address).toBe('');
    })

  });
})