import { describe, it, expect, afterEach } from 'vitest';
import { 
  formatCurrency, 
  formatDate, 
  formatDateLong, 
  formatDateTime 
} from './formatters';

// Mock the Intl.NumberFormat and Date.toLocaleDateString
const originalNumberFormat = Intl.NumberFormat;
const originalDateToLocaleDateString = Date.prototype.toLocaleDateString;
const originalDateToLocaleTimeString = Date.prototype.toLocaleTimeString;

describe('Formatters', () => {
  // Restore all mocks after each test
  afterEach(() => {
    Intl.NumberFormat = originalNumberFormat;
    Date.prototype.toLocaleDateString = originalDateToLocaleDateString;
    Date.prototype.toLocaleTimeString = originalDateToLocaleTimeString;
  });

  // formatCurrency tests
  describe('formatCurrency', () => {
    it('should format positive numbers as USD currency', () => {
      expect(formatCurrency(1234.56)).toBe('$1,234.56');
    });

    it('should format negative numbers as USD currency with negative sign', () => {
      expect(formatCurrency(-1234.56)).toBe('-$1,234.56');
    });

    it('should format zero as $0.00', () => {
      expect(formatCurrency(0)).toBe('$0.00');
    });

    it('should format very large numbers with commas as thousand separators', () => {
      expect(formatCurrency(1234567.89)).toBe('$1,234,567.89');
    });

    it('should format decimals correctly by rounding to 2 decimal places', () => {
      expect(formatCurrency(1234.567)).toBe('$1,234.57');
      expect(formatCurrency(1234.5)).toBe('$1,234.50');
    });
  });

  // formatDate tests
  describe('formatDate', () => {
    it('should format an ISO date string to short date format', () => {
      // Use a fixed date for testing: January 15, 2023
      const testDateISO = '2023-01-15T12:30:45Z';
      expect(formatDate(testDateISO)).toMatch(/Jan 15, 2023/);
    });

    it('should handle different date formats correctly', () => {
      expect(formatDate('2023/05/20')).toMatch(/May 20, 2023/);
    });

    it('should handle invalid dates gracefully', () => {
      // Implementation may vary, but it shouldn't throw an error
      expect(() => formatDate('invalid-date')).not.toThrow();
    });
  });

  // formatDateLong tests
  describe('formatDateLong', () => {
    it('should format an ISO date string to long date format with time', () => {
      // Use a fixed date for testing: January 15, 2023, 14:30
      const testDateISO = '2023-01-15T14:30:45Z';
      // The exact output may depend on the timezone, so we check for the key components
      const result = formatDateLong(testDateISO);
      expect(result).toContain('January 15, 2023');
      // Time part could vary by timezone, so just check if it's present
      expect(result).toMatch(/\d{1,2}:\d{2}/);
    });

    it('should handle different date formats correctly', () => {
      const result = formatDateLong('2023/05/20 15:45');
      expect(result).toContain('May 20, 2023');
      expect(result).toMatch(/\d{1,2}:\d{2}/);
    });

    it('should handle invalid dates gracefully', () => {
      // Implementation may vary, but it shouldn't throw an error
      expect(() => formatDateLong('invalid-date')).not.toThrow();
    });
  });

  // formatDateTime tests
  describe('formatDateTime', () => {
    it('should format an ISO date string to date time format with seconds', () => {
      // Use a fixed date for testing: January 15, 2023, 14:30:45
      const testDateISO = '2023-01-15T14:30:45Z';
      const result = formatDateTime(testDateISO);
      
      // The exact output may depend on the timezone, so we check for the key components
      expect(result).toMatch(/\d{1,2}\/\d{1,2}\/\d{4}/); // Date part
      expect(result).toMatch(/\d{1,2}:\d{2}:\d{2}/); // Time part with seconds
      expect(result).toMatch(/[AP]M/); // AM/PM indicator
    });

    it('should handle different date formats correctly', () => {
      const result = formatDateTime('2023/05/20 15:45:30');
      expect(result).toMatch(/\d{1,2}\/\d{1,2}\/\d{4}/); // Date part
      expect(result).toMatch(/\d{1,2}:\d{2}:\d{2}/); // Time part with seconds
      expect(result).toMatch(/[AP]M/); // AM/PM indicator
    });

    it('should handle invalid dates gracefully', () => {
      // Implementation may vary, but it shouldn't throw an error
      expect(() => formatDateTime('invalid-date')).not.toThrow();
    });
  });
}); 