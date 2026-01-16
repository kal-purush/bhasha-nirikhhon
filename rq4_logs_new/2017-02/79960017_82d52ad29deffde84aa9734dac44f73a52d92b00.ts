import { Address } from './address.model';

describe('Address', () => {
  let address: Address;
  let emptyAddress: Address;

  describe('instance created with full deatils', () => {
    beforeEach(() => {
      address = new Address({'street': "123 North Kings Drive", 'city': 'Myrtle Beach', 'state': 'SC', 'zip': '29577' });
      emptyAddress = new Address();
    });

    it('has street', () => {
      expect(address.street).toBe('123 North Kings Drive');
    });

    it('has city', () => {
      expect(address.city).toBe('Myrtle Beach');
    });

    it('has state', () => {
      expect(address.state).toBe('SC');
    });

    it('has zip code', () => {
      expect(address.zip).toBe('29577');
    });
  });

  describe('instance created with missing details', () => {
    beforeEach(() => {
      emptyAddress = new Address();
    })

    it('has empty street field', () => {
      expect(emptyAddress.street).toBe('');
    });

    it('has empty city field', () => {
      expect(emptyAddress.city).toBe('');
    });

    it('has empty state field', () => {
      expect(emptyAddress.state).toBe('');
    });

    it('has empty zip code field', () => {
      expect(emptyAddress.zip).toBe('');
    });
  })

})