import { describe, expect, it } from '@jest/globals';

import { getTotalNumberOfPagesIntoPagination, getSkipIntoPagination } from './config';

describe('getTotalNumberOfPagesIntoPagination', () => {
    it('should calculate total pages based on total and limit when limit > 0', () => {
        const total = 50;
        const limit = 10;
        const perPage = 5;
        const result = getTotalNumberOfPagesIntoPagination(total, limit, perPage);

        expect(result).toBe(5);
    });

    it('should calculate total pages based on total and perPage when limit is 0', () => {
        const total = 50;
        const limit = 0;
        const perPage = 5;
        const result = getTotalNumberOfPagesIntoPagination(total, limit, perPage);

        expect(result).toBe(10);
    });

    it('should round up total pages to the nearest integer', () => {
        const total = 55;
        const limit = 9;
        const perPage = 5;
        const result = getTotalNumberOfPagesIntoPagination(total, limit, perPage);

        expect(result).toBe(7);
    });
});

describe('getSkipIntoPagination', () => {
    it('should return 0 for the first page', () => {
        const current_page = 1;
        const per_page = 10;
        const pages = 5;
        const total = 50;
        const result = getSkipIntoPagination(current_page, per_page, pages, total);

        expect(result).toBe(0);
    });

    it('should return per_page value for the second page', () => {
        const current_page = 2;
        const per_page = 10;
        const pages = 5;
        const total = 50;
        const result = getSkipIntoPagination(current_page, per_page, pages, total);

        expect(result).toBe(10);
    });

    it('should return total value for the last page', () => {
        const current_page = 5;
        const per_page = 10;
        const pages = 5;
        const total = 50;
        const result = getSkipIntoPagination(current_page, per_page, pages, total);

        expect(result).toBe(50);
    });

    it('should return current_page * per_page for other pages', () => {
        const current_page = 3;
        const per_page = 10;
        const pages = 5;
        const total = 50;
        const result = getSkipIntoPagination(current_page, per_page, pages, total);

        expect(result).toBe(30);
    });
});