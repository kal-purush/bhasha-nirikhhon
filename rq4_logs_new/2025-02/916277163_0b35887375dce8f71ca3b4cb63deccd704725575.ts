import { describe, expect, it } from '@jest/globals';

import ExpenseGroup from './expenseGroup';
import { Error, ERROR_STATUS_CODE } from '@repo/services/error/error';
import type { ExpenseGroupEntity } from './interface';

describe('ExpenseGroup', () => {
  describe('Constructor', () => {
    it('should create an instance with all parameters when valid data is provided', () => {
      const params = {
        id: '1',
        name: 'Travel Expenses',
        created_at: new Date('2023-01-01'),
        updated_at: new Date('2023-01-02'),
        deleted_at: undefined,
      };

      const expenseGroup = new ExpenseGroup(params);

      expect(expenseGroup.id).toBe(params.id);
      expect(expenseGroup.name).toBe(params.name);
      expect(expenseGroup.created_at).toEqual(params.created_at);
      expect(expenseGroup.updated_at).toEqual(params.updated_at);
      expect(expenseGroup.deleted_at).toBe(params.deleted_at);
    });

    it('should create an instance with minimal valid data', () => {
      const params = {
        name: 'Equipment Expenses',
      };

      const expenseGroup = new ExpenseGroup(params);

      expect(expenseGroup.name).toBe(params.name);
      expect(expenseGroup.id).toBeUndefined();
      expect(expenseGroup.created_at).toBeUndefined();
      expect(expenseGroup.updated_at).toBeUndefined();
      expect(expenseGroup.deleted_at).toBeUndefined();
    });

    it('should throw an error when name is missing', () => {
      const params = {
        id: '1',
        created_at: new Date('2023-01-01'),
      };

      expect(() => new ExpenseGroup(params as ExpenseGroupEntity)).toThrow(
        new Error({
          message: 'name is required',
          statusCode: ERROR_STATUS_CODE.CONFLICT_EXCEPTION,
        }),
      );
    });

    it('should allow instantiation with no parameters', () => {
      const expenseGroup = new ExpenseGroup();

      expect(expenseGroup.id).toBeUndefined();
      expect(expenseGroup.name).toBeUndefined();
      expect(expenseGroup.created_at).toBeUndefined();
      expect(expenseGroup.updated_at).toBeUndefined();
      expect(expenseGroup.deleted_at).toBeUndefined();
    });

    it('should set default values for optional fields when they are not provided', () => {
      const params = {
        name: 'Default Expenses',
      };

      const expenseGroup = new ExpenseGroup(params);

      expect(expenseGroup.name).toBe(params.name);
      expect(expenseGroup.created_at).toBeUndefined();
      expect(expenseGroup.updated_at).toBeUndefined();
      expect(expenseGroup.deleted_at).toBeUndefined();
    });
  });
});