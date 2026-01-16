import { beforeEach, jest,  describe, expect, it } from '@jest/globals';

import { FinanceService } from './financeService';
import { Nest } from '../api';

describe('FinanceService', () => {
    let financeService: FinanceService;
    let nestMock: jest.Mocked<Nest>;

    beforeEach(() => {
        nestMock = {} as jest.Mocked<Nest>;

        financeService = new FinanceService(nestMock);
    });

    it('should be instantiated correctly', () => {
        expect(financeService).toBeDefined();
    });

    it('should receive the Nest dependency in the constructor\n', () => {
        expect(financeService['nest']).toBe(nestMock);
    });
});