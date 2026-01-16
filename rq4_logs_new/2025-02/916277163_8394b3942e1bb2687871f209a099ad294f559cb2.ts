import { describe, expect, it } from '@jest/globals';

import ExpenseCategoryType from './expenseCategoryType';
import { Error, ERROR_STATUS_CODE } from '@repo/services/error/error';
import type { ExpenseCategoryTypeEntity } from './interface';

describe('ExpenseCategoryType', () => {
    describe('Constructor', () => {
        it('should create an instance with all parameters when valid data is provided', () => {
            const params = {
                id: '1',
                name: 'Office Supplies',
                created_at: new Date('2023-01-01'),
                updated_at: new Date('2023-01-02'),
                deleted_at: undefined,
            };

            const expenseCategoryType = new ExpenseCategoryType(params);

            expect(expenseCategoryType.id).toBe(params.id);
            expect(expenseCategoryType.name).toBe(params.name);
            expect(expenseCategoryType.created_at).toEqual(params.created_at);
            expect(expenseCategoryType.updated_at).toEqual(params.updated_at);
            expect(expenseCategoryType.deleted_at).toBe(params.deleted_at);
        });

        it('should create an instance with only the required `name` field provided', () => {
            const params = {
                name: 'Travel Expenses',
            };

            const expenseCategoryType = new ExpenseCategoryType(params);

            expect(expenseCategoryType.name).toBe(params.name);
            expect(expenseCategoryType.id).toBeUndefined();
            expect(expenseCategoryType.created_at).toBeUndefined();
            expect(expenseCategoryType.updated_at).toBeUndefined();
            expect(expenseCategoryType.deleted_at).toBeUndefined();
        });

        it('should throw an error when `name` is not provided', () => {
            const params = {
                id: '1',
                created_at: new Date('2023-01-01'),
                updated_at: new Date('2023-01-02'),
            };

            expect(() => new ExpenseCategoryType(params as ExpenseCategoryTypeEntity)).toThrow(
                new Error({
                    message: 'name is required',
                    statusCode: ERROR_STATUS_CODE.CONFLICT_EXCEPTION,
                }),
            );
        });

        it('should allow instantiation with no parameters', () => {
            const expenseCategoryType = new ExpenseCategoryType();

            expect(expenseCategoryType.id).toBeUndefined();
            expect(expenseCategoryType.name).toBeUndefined();
            expect(expenseCategoryType.created_at).toBeUndefined();
            expect(expenseCategoryType.updated_at).toBeUndefined();
            expect(expenseCategoryType.deleted_at).toBeUndefined();
        });

        it('should set optional fields to undefined when they are not provided', () => {
            const params = {
                name: 'Marketing Expenses',
            };

            const expenseCategoryType = new ExpenseCategoryType(params);

            expect(expenseCategoryType.name).toBe(params.name);
            expect(expenseCategoryType.created_at).toBeUndefined();
            expect(expenseCategoryType.updated_at).toBeUndefined();
            expect(expenseCategoryType.deleted_at).toBeUndefined();
        });
    });
});