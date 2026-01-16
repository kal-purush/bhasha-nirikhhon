import { FieldValidationSpy } from '../test/mock-field-validation';
import { ValidationComposite } from './validation-composite';

describe('ValidationComposite', () => {
  test('should return error if any validation fails', () => {
    const fieldValidationSpy = new FieldValidationSpy('field');
    const fieldValidationSpy2 = new FieldValidationSpy('field');
    fieldValidationSpy2.error = new Error('any error');
    const sut = new ValidationComposite([fieldValidationSpy, fieldValidationSpy2]);
    const error = sut.validate('field', 'value');
    expect(error).toBe('any error');
  });
});