import { RutIsMalformedError } from '@/errors';

test('...', () => {
  const throwError = () => {
    throw new RutIsMalformedError();
  };

  try {
    throwError();
  } catch (error) {
    expect(error).toBeInstanceOf(RutIsMalformedError);
  }
});