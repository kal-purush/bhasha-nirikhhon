import { Validation } from '@falsy/metacore';
import { isJson } from '../rules';
import { validateField } from '../validate';

describe('isJson', () => {
  const validation: Validation[] = [isJson('Not valid JSON')];

  it('should return an error message when the field value is not json', () => {
    const errorMessage = validateField({}, { type: 'text', value: '{ hey.', validation });
    expect(errorMessage).toEqual('Not valid JSON');
  });

  it('should not return an error message when the field value is valid JSON', () => {
    const errorMessage = validateField({}, { type: 'text', value: '{ "hello": "world" }', validation });
    expect(errorMessage).toEqual(undefined);
  });

  it('should throw error if value is not string', () => {
    const errorMessage = validateField({}, { type: 'text', value: 1, validation });
    expect(errorMessage).toEqual('Not valid JSON');
  });
});