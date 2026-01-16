import { IContact } from '@/shared/interface';
import { sortContacts } from '@/utils/sort';
import { isNameValid, isValidPhoneNumber } from '@/utils/validator';

const sampleContacts: IContact[] = [
  {
    created_at: '2023-09-16',
    first_name: 'John',
    last_name: 'Doe',
    id: 1,
    phones: [],
  },
  {
    created_at: '2023-09-17',
    first_name: 'Alice',
    last_name: 'Wonder',
    id: 2,
    phones: [],
  },
  {
    created_at: '2023-09-18',
    first_name: 'Bob',
    last_name: 'Builder',
    id: 3,
    phones: [],
  },
];

describe('sortContacts', () => {
  it('should sort contacts by first_name in ascending order', () => {
    const sortedContacts = sortContacts(sampleContacts);
    expect(sortedContacts[0].first_name).toBe('Alice');
    expect(sortedContacts[1].first_name).toBe('Bob');
    expect(sortedContacts[2].first_name).toBe('John');
  });

  it('should not modify the original contacts array', () => {
    const originalContacts = [...sampleContacts];
    sortContacts(sampleContacts);
    expect(sampleContacts).toEqual(originalContacts);
  });
});

describe('isNameValid', () => {
  it('should return true for a valid name', () => {
    expect(isNameValid('John Doe')).toBe(true);
  });

  it('should return false for an invalid name with special characters', () => {
    expect(isNameValid('John@Doe')).toBe(false);
  });

  it('should return false for an invalid name with numbers', () => {
    expect(isNameValid('John123')).toBe(false);
  });
});

describe('isValidPhoneNumber', () => {
  it('should return true for a valid phone number', () => {
    expect(isValidPhoneNumber('123456789')).toBe(true);
  });

  it('should return true for a valid phone number with leading +', () => {
    expect(isValidPhoneNumber('+123456789')).toBe(true);
  });

  it('should return false for an invalid phone number with + anywhere else other than first character', () => {
    expect(isValidPhoneNumber('1234+56789')).toBe(false);
  });

  it('should return false for an invalid phone number with other characters', () => {
    expect(isValidPhoneNumber('123-456-7890')).toBe(false);
  });

  it('should return false for an invalid phone number with other characters', () => {
    expect(isValidPhoneNumber('1number9')).toBe(false);
  });
});