import { describe, expect, test } from 'bun:test';
import { FieldValidatorPrivateFixtures as FieldValidatorFixtures } from './test-fixtures';
import { DtoFieldValidator } from '../../../../src/domain/validator/field-validator/dto-field-validator';
import { DTO } from '../../../../src/domain/dto';
import { CannotBeUndefinedValidationRule } from '../../../../src/domain/validator/rules/assert-rules/cannot-be-undefined.a-rule';
import { CanBeNullValidationRule } from '../../../../src/domain/validator/rules/nullable-rules/can-be-only-null.n-rule';
import { ValidationRule } from '../../../../src/domain/validator/rules/validation-rule';

describe('DTO Tests', () => {
  describe('Test cases when isNotArray', () => {
    describe('Test cases when isNotArray and attributes are required', () => {
      const sut = new DtoFieldValidator('phones', true, { isArray: false }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);

      test('Success, a valid value has arrived', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        );
        expect(result.isSuccess()).toBe(true);
        expect(result.value).toEqual(undefined);
      });

      test('Failure, one of the attributes is not a valid value', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 5454, noOutField: 'info' },
        );
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          type: [
            {
              text: 'Значение должно быть строковым значением',
              hint: {},
            },
          ],
        });
      });

      test('Failure, several attributes are not valid', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 5454, noOutField: undefined },
        );
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          noOutField: [
            {
              hint: {},
              text: 'Значение не должно быть undefined или null',
            },
          ],
          type: [
            {
              text: 'Значение должно быть строковым значением',
              hint: {},
            },
          ],
        });
      });

      test('Failure, literal value together with dto', () => {
        const result = sut.validate(2);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: [
            {
              text: 'Значение должно быть объектом',
              hint: {},
            },
          ],
        });
      });

      test('Failure, came undefined, null instead of dto object', () => {
        const valuesToCheck = [undefined, null];
        valuesToCheck.forEach((value) => {
          const result = sut.validate(value);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: [
              {
                text: 'Значение не должно быть undefined или null',
                hint: {},
              },
            ],
          });
        });
      });

      test('Failure, came an array of objects together dto object', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: [
            {
              text: 'Значение должно быть объектом',
              hint: {},
            },
          ],
        });
      });

      describe('Cannot-be-undefined and can-be-null tests', () => {
        class Cannotbe extends DtoFieldValidator< 'phones', true, false, DTO> {
          protected getRequiredOrNullableRules(): Array<ValidationRule<'assert', unknown> | ValidationRule<'nullable', unknown>> {
            return [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()];
          }
        }
        const sutCanBe = new Cannotbe('phones', true, { isArray: false }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);

        test('Success, a valid value has arrived', () => {
          const result = sutCanBe.validate(
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          );
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Success, received null', () => {
          const result = sutCanBe.validate(null);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Failure, dto arrived with errors in attributes', () => {
          const result = sutCanBe.validate(
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 3 },
          );
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            noOutField: [
              {
                text: 'Значение должно быть строковым значением',
                hint: {},
              },
            ],
          });
        });

        test('Failure, arrived undefined', () => {
          const result = sutCanBe.validate(undefined);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: [
              {
                text: 'Значение не должно быть undefined',
                hint: {},
              },
            ],
          });
        });
      });
    });

    describe('Test cases when isNotArray and attributes are not required', () => {
      const sut = new DtoFieldValidator('phones', false, { isArray: false }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);

      test('Success, a valid value has arrived', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        );
        expect(result.isSuccess()).toBe(true);
        expect(result.value).toBe(undefined);
      });

      test('Failure, one of the attributes is not a valid value', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 5454, noOutField: 'info' },
        );
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          type: [
            {
              text: 'Значение должно быть строковым значением',
              hint: {},
            },
          ],
        });
      });

      test('Failure, several attributes are not valid', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 5454, noOutField: undefined },
        );
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          noOutField: [
            {
              hint: {},
              text: 'Значение не должно быть undefined или null',
            },
          ],
          type: [
            {
              text: 'Значение должно быть строковым значением',
              hint: {},
            },
          ],
        });
      });

      test('Failure, instead of the dto object a literal value was received', () => {
        const result = sut.validate(2);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: [
            {
              text: 'Значение должно быть объектом',
              hint: {},
            },
          ],
        });
      });

      test('Failure, together the value came undefined null', () => {
        const valuesToCheck = [undefined, null];
        valuesToCheck.forEach((value) => {
          const result = sut.validate(value);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });
      });

      test('Failure, DTO array arrived', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: [
            {
              text: 'Значение должно быть объектом',
              hint: {},
            },
          ],
        });
      });

      describe('Cannot-be-undefined and can-be-null tests', () => {
        class Cannotbe extends DtoFieldValidator< 'phones', false, false, DTO> {
          protected getRequiredOrNullableRules(): Array<ValidationRule<'assert', unknown> | ValidationRule<'nullable', unknown>> {
            return [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()];
          }
        }
        const sutCanBe = new Cannotbe('phones', false, { isArray: false }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);

        test('Success, valid value arrived', () => {
          const result = sutCanBe.validate(
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          );
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Success, came null value', () => {
          const result = sutCanBe.validate(null);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Failure, dto arrived with errors in attributes', () => {
          const result = sutCanBe.validate(
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 3 },
          );
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            noOutField: [
              {
                text: 'Значение должно быть строковым значением',
                hint: {},
              },
            ],
          });
        });

        test('Failure, came undefined value', () => {
          const result = sutCanBe.validate(undefined);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: [
              {
                text: 'Значение не должно быть undefined',
                hint: {},
              },
            ],
          });
        });
      });
    });
  });

  describe('Test cases when isArray', () => {
    describe('Test cases when isArray and attributes are required', () => {
      const sut = new DtoFieldValidator('phones', true, { isArray: true }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);
      const valuesToCheck = [undefined, null];

      test('Success, a valid data array arrived from dto', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        ]);
        expect(result.isSuccess()).toBe(true);
        expect(result.value).toBe(undefined);
      });

      test('Failure, came as undefined and null', () => {
        valuesToCheck.forEach((value) => {
          const result = sut.validate(value);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: [
              {
                text: 'Значение не должно быть undefined или null',
                hint: {},
              },
            ],
          });
        });
      });

      test('Failure, a regular dto object arrived', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        );
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: [
            {
              text: 'Значение должно быть массивом данных',
              hint: {},
            },
          ],
        });
      });

      test('Failure, an array arrived with errors', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 2, noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'mobile', noOutField: 'info' },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            0: {
              type: [
                {
                  hint: {},
                  text: 'Значение должно быть строковым значением',
                },
              ],
            },
          },
        });
      });

      test('Failure, an array arrived with several errors', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 2, noOutField: true },
          { number: '+7-555-879-11-02', type: 'mobile', noOutField: 'info' },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            0: {
              noOutField: [
                {
                  hint: {},
                  text: 'Значение должно быть строковым значением',
                },
              ],
              type: [
                {
                  hint: {},
                  text: 'Значение должно быть строковым значением',
                },
              ],
            },
          },
        });
      });

      test('Failure, arrays arrived with errors', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 2, noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'mobile', noOutField: true },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            0: {
              type: [
                {
                  text: 'Значение должно быть строковым значением',
                  hint: {},
                },
              ],
            },
            1: {
              noOutField: [
                {
                  text: 'Значение должно быть строковым значением',
                  hint: {},
                },
              ],
            },
          },
        });
      });

      test('Failure, a data array with a literal value arrived', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
          2,
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            1: {
              ___whole_value_validation_error___: [
                {
                  text: 'Значение должно быть объектом',
                  hint: {},
                },
              ],
            },
          },
        });
      });

      describe('Cannot-be-undefined and can-be-null tests', () => {
        class Cannotbe extends DtoFieldValidator< 'phones', true, true, DTO> {
          protected getRequiredOrNullableRules(): Array<ValidationRule<'assert', unknown> | ValidationRule<'nullable', unknown>> {
            return [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()];
          }
        }
        const sutCanBe = new Cannotbe('phones', true, { isArray: true }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);

        test('Success, valid value arrived', () => {
          const result = sutCanBe.validate([
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          ]);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Success, came as null', () => {
          const result = sutCanBe.validate(null);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Failure, dto arrived with errors in attributes', () => {
          const result = sutCanBe.validate([
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 3 },
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          ]);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: {
              0: {
                noOutField: [
                  {
                    text: 'Значение должно быть строковым значением',
                    hint: {},
                  },
                ],
              },
            },
          });
        });

        test('Failure, came as undefined value', () => {
          const result = sutCanBe.validate(undefined);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: [
              {
                text: 'Значение не должно быть undefined',
                hint: {},
              },
            ],
          });
        });

        test('Failure, one of the elements is not a dto object', () => {
          const result = sutCanBe.validate([
            2,
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          ]);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: {
              0: {
                ___whole_value_validation_error___: [
                  {
                    text: 'Значение должно быть объектом',
                    hint: {},
                  },
                ],
              },
            },
          });
        });
      });
    });

    describe('Test cases when isArray and attributes are not required', () => {
      const sut = new DtoFieldValidator('phones', false, { isArray: true }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);
      const valuesToCheck = [undefined, null];

      test('Success, arrived valid value', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        ]);
        expect(result.isSuccess()).toBe(true);
        expect(result.value).toBe(undefined);
      });

      test('Success, came as undefined and null value', () => {
        valuesToCheck.forEach((value) => {
          const result = sut.validate(value);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });
      });

      test('Failure, a simple dto object arrived instead of an array with data', () => {
        const result = sut.validate(
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
        );
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: [
            {
              text: 'Значение должно быть массивом данных',
              hint: {},
            },
          ],
        });
      });

      test('Failure, an array arrived with errors in the attributes', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 2, noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'mobile', noOutField: 'info' },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            0: {
              type: [
                {
                  hint: {},
                  text: 'Значение должно быть строковым значением',
                },
              ],
            },
          },
        });
      });

      test('Failure, an array arrived with several errors', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 2, noOutField: true },
          { number: '+7-555-879-11-02', type: 'mobile', noOutField: 'info' },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            0: {
              noOutField: [
                {
                  hint: {},
                  text: 'Значение должно быть строковым значением',
                },
              ],
              type: [
                {
                  hint: {},
                  text: 'Значение должно быть строковым значением',
                },
              ],
            },
          },
        });
      });

      test('Failure, an array of data was received with errors in the attributes in both arrays', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 2, noOutField: 'info' },
          { number: '+7-555-879-11-02', type: 'mobile', noOutField: true },
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            0: {
              type: [
                {
                  text: 'Значение должно быть строковым значением',
                  hint: {},
                },
              ],
            },
            1: {
              noOutField: [
                {
                  text: 'Значение должно быть строковым значением',
                  hint: {},
                },
              ],
            },
          },
        });
      });

      test('Failure, a data array has arrived where one of the elements has a literal value', () => {
        const result = sut.validate([
          { number: '+7-555-879-11-02', type: 'work', noOutField: 'info' },
          2,
        ]);
        expect(result.isFailure()).toBe(true);
        expect(result.value).toEqual({
          ___whole_value_validation_error___: {
            1: {
              ___whole_value_validation_error___: [
                {
                  text: 'Значение должно быть объектом',
                  hint: {},
                },
              ],
            },
          },
        });
      });

      describe('Cannot-be-undefined and can-be-null test', () => {
        class Cannotbe extends DtoFieldValidator< 'phones', false, true, DTO> {
          protected getRequiredOrNullableRules(): Array<ValidationRule<'assert', unknown> | ValidationRule<'nullable', unknown>> {
            return [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()];
          }
        }
        const sutCanBe = new Cannotbe('phones', false, { isArray: true }, 'dto', FieldValidatorFixtures.phoneAttrsValidatorMap);

        test('Success, valid value arrived', () => {
          const result = sutCanBe.validate([
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          ]);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Success, came as null', () => {
          const result = sutCanBe.validate(null);
          expect(result.isSuccess()).toBe(true);
          expect(result.value).toBe(undefined);
        });

        test('Failure, an array arrived with errors in the attributes', () => {
          const result = sutCanBe.validate([
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 3 },
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          ]);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: {
              0: {
                noOutField: [
                  {
                    text: 'Значение должно быть строковым значением',
                    hint: {},
                  },
                ],
              },
            },
          });
        });

        test('Failure, came as undefined value', () => {
          const result = sutCanBe.validate(undefined);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: [
              {
                text: 'Значение не должно быть undefined',
                hint: {},
              },
            ],
          });
        });

        test('Failure, one of the elements is not a dto object', () => {
          const result = sutCanBe.validate([
            2,
            { number: '+7-777-287-81-82', type: 'mobile', noOutField: 'empty info' },
          ]);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            ___whole_value_validation_error___: {
              0: {
                ___whole_value_validation_error___: [
                  {
                    text: 'Значение должно быть объектом',
                    hint: {},
                  },
                ],
              },
            },
          });
        });
      });
    });
  });
});