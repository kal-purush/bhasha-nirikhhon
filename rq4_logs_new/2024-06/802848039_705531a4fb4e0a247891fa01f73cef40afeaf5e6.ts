import { getProfileLetters } from './get-profile-letters';

describe('get profile letters', () => {
  test('should split by .', () => {
    const res = getProfileLetters({
      email: 'michael.trembovler@gmail.com',
    });

    expect(res).toEqual('MT');
  });

  test('should split by -', () => {
    const res = getProfileLetters({
      email: 'michael.trembovler@gmail.com',
      name: 'Michael-Trembovler',
    });

    expect(res).toEqual('MT');
  });

  test('should split by _', () => {
    const res = getProfileLetters({
      email: 'michael.trembovler@gmail.com',
      name: 'Michael-Trembovler',
    });

    expect(res).toEqual('MT');
  });

  test('should split by "', () => {
    const res = getProfileLetters({
      email: 'miketrem640@gmail.com',
      name: '"',
    });

    expect(res).toEqual('64');
  });

  test('should split by _', () => {
    const res = getProfileLetters({
      email: 'mictrwork@gmail.com',
      name: 'Michael Trembovler',
    });

    expect(res).toEqual('MT');
  });

  test('should split by space', () => {
    const res = getProfileLetters({
      email: 'michael.trembovler@gmail.com',
      name: 'Michael Trembovler',
    });

    expect(res).toEqual('MT');
  });

  test('should return first 2 letters if no separator', () => {
    const res = getProfileLetters({
      email: 'michael.trembovler@gmail.com',
      name: 'MichaelTrembovler',
    });

    expect(res).toEqual('MI');
  });
  test('should return first 2 letters if no separator email', () => {
    const res = getProfileLetters({
      email: 'admin@gmail.com',
    });

    expect(res).toEqual('AD');
  });
  test('should return email if empty username', () => {
    const res = getProfileLetters({
      email: 'admin@gmail.com',
      name: '',
    });

    expect(res).toEqual('AD');
  });

  test('should work with empty names', () => {
    const res = getProfileLetters({
      email: 'admin@gmail.com',
      name: 'E',
    });

    expect(res).toEqual('AD');
  });

  test('should work with short names', () => {
    const res = getProfileLetters({
      email: 'admin@gmail.com',
      name: null,
    });

    expect(res).toEqual('AD');
  });

  test('should work with empty names', () => {
    const res = getProfileLetters({
      email: 'admin@gmail.com',
      name: '',
    });

    expect(res).toEqual('AD');
  });
});