import { describe, expect, it } from '@jest/globals';

import { Error, ERROR_STATUS_CODE } from '@repo/services/error/error';

import ExpenseCategory from './expenseCategory';
import type { ExpenseCategoryEntity } from './interface';

describe('ExpenseCategory', () => {
  describe('Constructor', () => {
    it('should create an instance with all parameters when valid data is provided', () => {
      const params = {
        id: '123',
        name: 'Travel',
        type: {
          id: '1',
          name: 'Office Supplies',
          created_at: new Date('2023-01-01'),
          updated_at: new Date('2023-01-02'),
          deleted_at: undefined,
        },
        created_at: new Date('2023-01-01'),
        updated_at: new Date('2023-01-02'),
        deleted_at: undefined,
      };

      const expenseCategory = new ExpenseCategory(params);

      expect(expenseCategory.id).toBe(params.id);
      expect(expenseCategory.name).toBe(params.name);
      expect(expenseCategory.type).toBe(params.type);
      expect(expenseCategory.created_at).toEqual(params.created_at);
      expect(expenseCategory.updated_at).toEqual(params.updated_at);
      expect(expenseCategory.deleted_at).toBe(params.deleted_at);
    });

    it('should create an instance with only required fields (name and type)', () => {
      const params = {
        name: 'Office Supplies',
        type: {
          id: '1',
          name: 'Operational',
          created_at: new Date('2023-01-01'),
          updated_at: new Date('2023-01-02'),
          deleted_at: undefined,
        },
      };

      const expenseCategory = new ExpenseCategory(params);

      expect(expenseCategory.name).toBe(params.name);
      expect(expenseCategory.type).toBe(params.type);
      expect(expenseCategory.id).toBeUndefined();
      expect(expenseCategory.created_at).toBeUndefined();
      expect(expenseCategory.updated_at).toBeUndefined();
      expect(expenseCategory.deleted_at).toBeUndefined();
    });

    it('should throw an error when name is missing', () => {
      const params = {
        name: undefined,
        type: {
          id: '1',
          name: 'Business',
          created_at: new Date('2023-01-01'),
          updated_at: new Date('2023-01-02'),
          deleted_at: undefined,
        },
        updated_at: new Date(),
        deleted_at: undefined,
      };

      expect(() => new ExpenseCategory(params)).toThrow(
        new Error({
          statusCode: ERROR_STATUS_CODE.CONFLICT_EXCEPTION,
          message: 'name is required',
        }),
      );
    });

    it('should throw an error when type is missing', () => {
      const params = {
        name: 'Marketing',
      };

      expect(
        () => new ExpenseCategory(params as ExpenseCategoryEntity),
      ).toThrow(
        new Error({
          statusCode: ERROR_STATUS_CODE.CONFLICT_EXCEPTION,
          message: 'type is required',
        }),
      );
    });

    it('should allow instantiation with no parameters', () => {
      const expenseCategory = new ExpenseCategory();

      expect(expenseCategory.id).toBeUndefined();
      expect(expenseCategory.name).toBeUndefined();
      expect(expenseCategory.type).toBeUndefined();
      expect(expenseCategory.created_at).toBeUndefined();
      expect(expenseCategory.updated_at).toBeUndefined();
      expect(expenseCategory.deleted_at).toBeUndefined();
    });
  });
});