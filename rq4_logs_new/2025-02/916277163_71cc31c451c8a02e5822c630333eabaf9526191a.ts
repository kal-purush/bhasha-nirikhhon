import { describe, expect, it, jest } from '@jest/globals';
import {
  getCurrentMonth,
  getMonthByIndex,
  getMonthIndex,
  validateMonth,
} from './config';

import { EMonth } from '../../api/nest/finance';

describe('Month Utils', () => {
  describe('getCurrentMonth', () => {
    it('should return the current month in uppercase format', () => {
      const mockDate = new Date(2023, 0);
      jest
        .spyOn(global, 'Date')
        .mockImplementation(() => mockDate);

      const currentMonth = getCurrentMonth();
      expect(currentMonth).toBe('JANUARY');
    });
  });

  describe('getMonthIndex', () => {
    it('should return the correct index for a valid month', () => {
      const month: EMonth = 'JANUARY' as EMonth;
      const index = getMonthIndex(month);
      expect(index).toBe(0);
    });

    it('should return -1 for an invalid month', () => {
      const invalidMonth: EMonth = 'INVALIDMONTH' as EMonth;
      const index = getMonthIndex(invalidMonth);
      expect(index).toBe(-1);
    });
  });

  describe('getMonthByIndex', () => {
    it('should return the correct month for a valid index', () => {
      const index = 2;
      const month = getMonthByIndex(index);
      expect(month).toBe('march');
    });

    it('should return undefined for an out-of-range index', () => {
      const index = 12;
      const month = getMonthByIndex(index);
      expect(month).toBeUndefined();
    });
  });

  describe('validateMonth', () => {
    it('should not throw an error for a valid month', () => {
      const validMonth: EMonth = 'DECEMBER' as EMonth;
      expect(() => validateMonth(validMonth)).not.toThrow();
    });

    it('should throw an error for an invalid month', () => {
      const invalidMonth: EMonth = 'INVALIDMONTH' as EMonth;
      expect(() => validateMonth(invalidMonth)).toThrow(
        'The month provided is invalid: INVALIDMONTH',
      );
    });
  });
});