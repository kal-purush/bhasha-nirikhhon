/* eslint-disable function-paren-newline */
import { describe, expect, test } from 'bun:test';
import { LiteralFieldValidator } from '../../../../src/domain/validator/field-validator/literal-field-validator';
import { StringChoiceValidationRule } from '../../../../src/domain/validator/rules/validate-rules/string/string-choice.v-rule';
import { DtoFieldValidator } from '../../../../src/domain/validator/field-validator/dto-field-validator';
import { FieldValidatorPrivateFixtures } from './test-fixtures';

describe('проверки неправильной конфигурации массива для LiteralFieldValidator', () => {
  test('максимальное количество элементов меньше, чем минимальное количество элементов', () => {
    const roles = ['admin', 'staffManager', 'officeChieff', 'saleManager'];
    const callback = () => (
      new LiteralFieldValidator(
        'roles', true, { isArray: true, maxElementsCount: 1, minElementsCount: 2 }, 'string', [new StringChoiceValidationRule(roles)],
      )
    );
    expect(callback).toThrow('not valid arrayConfig: min=2, max=1');
  });

  test('минимальное количество элементов меньше 0', () => {
    const roles = ['admin', 'staffManager', 'officeChieff', 'saleManager'];
    const callback = () => (
      new LiteralFieldValidator(
        'roles', true, { isArray: true, minElementsCount: -1 }, 'string', [new StringChoiceValidationRule(roles)],
      )
    );
    expect(callback).toThrow('not valid arrayConfig: min=-1, max=undefined');
  });

  test('максимальное количество элементов меньше 0', () => {
    const roles = ['admin', 'staffManager', 'officeChieff', 'saleManager'];
    const callback = () => (
      new LiteralFieldValidator(
        'roles', true, { isArray: true, maxElementsCount: -1 }, 'string', [new StringChoiceValidationRule(roles)],
      )
    );
    expect(callback).toThrow('not valid arrayConfig: min=undefined, max=-1');
  });
});

describe('проверки неправильной конфигурации массива для DtoFieldValidator', () => {
  test('максимальное количество элементов меньше, чем минимальное количество элементов', () => {
    const callback = () => (
      new DtoFieldValidator('phones', true, { isArray: true, maxElementsCount: 1, minElementsCount: 2 }, 'dto', FieldValidatorPrivateFixtures.phoneAttrsValidatorMap,
      ));
    expect(callback).toThrow('not valid arrayConfig: min=2, max=1');
  });

  test('минимальное количество элементов меньше 0', () => {
    const callback = () => (
      new DtoFieldValidator('phones', true, { isArray: true, minElementsCount: -1 }, 'dto', FieldValidatorPrivateFixtures.phoneAttrsValidatorMap,
      ));
    expect(callback).toThrow('not valid arrayConfig: min=-1, max=undefined');
  });

  test('максимальное количество элементов меньше 0', () => {
    const callback = () => (
      new DtoFieldValidator('phones', true, { isArray: true, maxElementsCount: -1 }, 'dto', FieldValidatorPrivateFixtures.phoneAttrsValidatorMap,
      ));
    expect(callback).toThrow('not valid arrayConfig: min=undefined, max=-1');
  });
});