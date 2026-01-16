import { describe, expect, test } from 'bun:test';
import { DtoFieldValidator } from '../../../../src/domain/validator/field-validator/dto-field-validator';
import { LiteralFieldValidator } from '../../../../src/domain/validator/field-validator/literal-field-validator';
import { GetFieldValidatorDataType } from '../../../../src/domain/validator/field-validator/types';
import { LiteralDataType } from '../../../../src/domain/validator/rules/types';
import { ContainedOnlyCharsValidationRule } from '../../../../src/domain/validator/rules/validate-rules/string/contained-only-chars';
import { FieldValidatorPrivateFixtures, FieldValidatorTestMocksPrivateFixtures } from './test-fixtures';
import { TrimStringLeadRule } from '../../../../src/domain/validator/rules/lead-rules/string/trim';
import { TrimEndStringLeadRule } from '../../../../src/domain/validator/rules/lead-rules/string/trim-end.l-rule';
import { TrimStartStringLeadRule } from '../../../../src/domain/validator/rules/lead-rules/string/trim-start.l-rule';
import { MaxCharsCountValidationRule } from '../../../../src/domain/validator/rules/validate-rules/string/max-chars-count.v-rule';
import { MinCharsCountValidationRule } from '../../../../src/domain/validator/rules/validate-rules/string/min-chars-count.v-rule';
import { CannotBeEmptyStringValidationRule } from '../../../../src/domain/validator/rules/assert-rules/cannot-be-empty-string.a-rule';
import { UUIDFormatValidationRule } from '../../../../src/domain/validator/rules/validate-rules/string/uuid-format.v-rule';
import { CannotBeUndefinedValidationRule } from '../../../../src/domain/validator/rules/assert-rules/cannot-be-undefined.a-rule';
import { CanBeNullValidationRule } from '../../../../src/domain/validator/rules/nullable-rules/can-be-only-null.n-rule';

  describe('isNotArray_tests, isRequired_tests', () => {
  const types: [GetFieldValidatorDataType<LiteralDataType>, unknown][] = [
    ['number', 5],
    ['string', 't'],
    ['boolean', false]
  ];
  
  types.forEach((type) => {
    describe('Валидированная значение обязательна', () => {

      test('Успех, на валидацию пришло явное значение', () => {
        const sut = new LiteralFieldValidator('fieldName', true, { isArray: false },type[0], []);
        const result = sut.validate(type[1]);
        expect(result.isSuccess()).toBe(true);
      });

      test('Провал, пришло undefined или null', () => {
        const values = [undefined, null];
        const sut = new LiteralFieldValidator('fieldName', true, { isArray: false },type[0], []);
        values.forEach((value) => {
          const result = sut.validate(value);
          expect(result.isFailure()).toBe(true);
          expect(result.value).toEqual({
            fieldName: [
              {
                text: "Значение не должно быть undefined или null",
                hint: {}
              }
            ]
          });
        });
      });
    });
  });

  describe('Валидированная значение обязательна', () => {
    test('Провал, пришло пустая строка', () => {
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false },'number', []);
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            hint: {},
            text: "Значение должно быть числовым"
          }
        ]
      });
    });
  
    test('Провал, пришло пустая строка', () => {
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false },'string', []);
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            hint: {},
            text: "Сротка обязательна к заполнению"
          }
        ]
      });
    });
    test('Провал, пришло пустая строка', () => {
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'boolean',[]);
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            hint: {},
            text: "Значение должно быть булевым"
          }
        ]
      });
  });
  });

  describe('Валидированная значение обязательна', () => {
    const sut = new DtoFieldValidator('email',  true, { isArray: false }, 'dto', FieldValidatorPrivateFixtures.emailAttrsValidatorMap);
    const values = [undefined, null];

    test('Успех, на валидацию пришло явное значение', () => {
      const result = sut.validate(
        { 
          value: "hetso@mail.ru",
          noOutField: "absolute"
        }
      );
      expect(result.isSuccess()).toBe(true);
    });
    
    test('Провал, пришло undefined или null', () => {
      values.forEach((value) => {
      const result = sut.validate(value);
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        ___whole_value_validation_error___:  [
          {
            text: "Значение не должно быть undefined или null",
            hint: {}
          }
        ]
      });
    });
  });

    test('Провал, пришло пустая строка', () => {
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        ___whole_value_validation_error___:  [
          {
            text: "Значение должно быть объектом",
            hint: {}
          }
        ]
      }); 
    });
  });

  /*delayu peredelat*/
  describe('Валидированная значение обязательна',()=>{
    test('Успех сработало приведение значения lead-rule',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [],[new TrimEndStringLeadRule()]);
      const result = sut.validate('Absolute   ');
      expect(result.isSuccess()).toBe(true);
    });
    test('Успех сработало приведение значения двух lead-rule',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [],[new TrimStartStringLeadRule(), new TrimEndStringLeadRule()]);
      const values = ['Absolute  ', '  Absolute', ' Absolute  '];
      values.forEach((value) => {
        const result = sut.validate(value);
      expect(result.isSuccess()).toBe(true);
      });
    });
    test('провал, пришло не валидное значение',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [new MaxCharsCountValidationRule(8)]);
      const values = ['Absolute ', 'Absolute  '];
      values.forEach((value) => {
        const result = sut.validate(value);
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Строка должна быть не больше {{maxCount}}",
            hint: {
              maxCount: 8
            }
          }
        ]
      });
      });
    });

    test('провал, пришло не валидное значение для нескольких правил',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [new MaxCharsCountValidationRule(8), new MinCharsCountValidationRule(3)]);
      const result = sut.validate('AG');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Строка должна быть не меньше {{minCount}}",
            hint: {
              minCount: 3
            }
          }
        ]
      });
    });

    test('провал, пришло не валидное значение для нескольких правил',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [new MinCharsCountValidationRule(10), new UUIDFormatValidationRule()]);
      const result = sut.validate('550e8400-e29b-41d4-a716-44665544000');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение должно соответствовать формату UUID",
            hint: {}
          },
        ]
      });
    });
  });
  describe('cannot-be-undefined, can-be-null',()=>{
    test('успех, пришло валидное значение',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate('Absolute');
      expect(result.isSuccess()).toBe(true);
    });
    test('успех, пришло undefined',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate(undefined);
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение не должно быть undefined или null",
            hint: {}
          },
        ]
      });
    });
    test('провал, пришло не валидное значение',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate(true);
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение должно быть строковым значением",
            hint: {}
          },
        ]
      });
    });
    test('успех, пришло null',()=>{
      const sut = new LiteralFieldValidator('fieldName',  true, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate(null);
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение не должно быть undefined или null",
            hint: {}
          },
        ]
      });
    });
  });
});



describe('isNotArray_tests, isNotRequired_tests', () => {
  const types: [GetFieldValidatorDataType<LiteralDataType>, unknown][] = [
    ['number', 5],
    ['string', 't'],
    ['boolean', false]
  ];
  types.forEach((type) => {
    describe('Валидированная значение необязательна', () => {
      test('Успех, на валидацию пришло явное значение', () => {
        const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, type[0],[]);
        const result = sut.validate(type[1]);
        expect(result.isSuccess()).toBe(true);
      });

      test('Провал, пришло undefined или null', () => {
        const values = [undefined, null];
        const sut = new LiteralFieldValidator('fieldName', false, { isArray: false },type[0],[]);
        values.forEach((value) => {
          const result = sut.validate(value);
          expect(result.isSuccess()).toBe(true);
        });
      });
  });
  });

  describe('Валидированная значение необязательна', () => {
    test('Провал, пришло пустая строка', () => {
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'number',[]);
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение должно быть числовым",
            hint: {}
          }
        ]
      });
    });
    test('Провал, пришло пустая строка', () => {
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string',[]);
      const result = sut.validate('');
      expect(result.isSuccess()).toBe(true);
    });

    test('неудачно, пришло пустая строка', () => {
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false },'boolean', []);
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение должно быть булевым",
            hint: {}
          }
        ]
      });
    });
  });


  describe('Валидированная значение необязательна', () => {
    const sut = new DtoFieldValidator('email', false, { isArray: false },'dto',  FieldValidatorPrivateFixtures.emailAttrsValidatorMap);
    const values = [undefined, null];
    test('Провал, пришло undefined или null', () => {
      values.forEach((value) => {
      const result = sut.validate(value);
      expect(result.isSuccess()).toBe(true);
    });
  });

    test('Провал, пришло пустая строка', () => {
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        ___whole_value_validation_error___:  [
          {
            text: "Значение должно быть объектом",
            hint: {}
          }
        ]
      }); 
    });
  });

  /*delayu*/
  describe('Валидированная значение обязательна',()=>{
    test('Успех сработало приведение значения lead-rule',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [],[new TrimStringLeadRule()]);
      const result = sut.validate('  Absolute  ');
      expect(result.isSuccess()).toBe(true);
    });
    test('Успех сработало приведение значения двух lead-rule',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [],[new TrimStartStringLeadRule(), new TrimEndStringLeadRule()]);
      const values = ['Absolute  ', '  Absolute', ' Absolute  '];
      values.forEach((value) => {
        const result = sut.validate(value);
      expect(result.isSuccess()).toBe(true);
      });
    });
    test('провал, пришло не валидное значение',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [new MaxCharsCountValidationRule(8)]);
      const values = ['Absolute ', 'Absolute  '];
      values.forEach((value) => {
        const result = sut.validate(value);
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Строка должна быть не больше {{maxCount}}",
            hint: {
              maxCount: 8
            }
          }
        ]
      });
      });
    });

    test('провал, пришло не валидное значение для нескольких правил',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [new MaxCharsCountValidationRule(8), new MinCharsCountValidationRule(3)]);
      const result = sut.validate('AG');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Строка должна быть не меньше {{minCount}}",
            hint: {
              minCount: 3
            }
          }
        ]
      });
    });

    test('провал, пришло не валидное значение для нескольких правил',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [new CannotBeEmptyStringValidationRule(), new MinCharsCountValidationRule(3)]);
      const result = sut.validate('');
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение должно быть не пустой строкой",
            hint: {}
          },
        ]
      });
    });
  });

  describe('cannot-be-undefined, can-be-null',()=>{
    test('успех, пришло валидное значение',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate('Absolute');
      expect(result.isSuccess()).toBe(true);
    });
    test('успех, пришло undefined',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate(undefined);
      expect(result.isSuccess()).toBe(true);
    });
    test('провал, пришло не валидное значение',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate(true);
      expect(result.isFailure()).toBe(true);
      expect(result.value).toEqual({
        fieldName: [
          {
            text: "Значение должно быть строковым значением",
            hint: {}
          },
        ]
      });
    });
    test('успех, пришло null',()=>{
      const sut = new LiteralFieldValidator('fieldName',  false, { isArray: false }, 'string', [new CannotBeUndefinedValidationRule(), new CanBeNullValidationRule()]);
      const result = sut.validate(null);
      expect(result.isSuccess()).toBe(true);
    });
  });
});