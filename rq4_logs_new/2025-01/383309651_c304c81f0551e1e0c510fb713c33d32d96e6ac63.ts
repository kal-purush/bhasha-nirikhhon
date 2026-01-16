// TODO: add tests for getNextMlkDay

import { getNextMlkDay } from './getNextMlkDay';
import { describe, it, expect } from 'vitest';

describe('getNextMlkDay', () => {
	it('should return Jan 20th 2025 for MLK Day if the current date is Jan 1st 2025', () => {
		const nextMlkDay = getNextMlkDay(new Date('2025-01-01 10:00:00'));
		expect(nextMlkDay.formattedDate).toBe('2025-01-20');
	});

	it('should return Jan 20th 2026 for MLK Day if the current date is Jan 1st 2026', () => {
		const nextMlkDay = getNextMlkDay(new Date('2026-01-01 10:00:00'));
		expect(nextMlkDay.formattedDate).toBe('2026-01-19');
	});

	it('should return Jan 20th 2027 for MLK Day if the current date is Jan 1st 2027', () => {
		const nextMlkDay = getNextMlkDay(new Date('2027-01-01 10:00:00'));
		expect(nextMlkDay.formattedDate).toBe('2027-01-18');
	});

	it('should return Jan 20th 2028 for MLK Day if the current date is Jan 1st 2028', () => {
		const nextMlkDay = getNextMlkDay(new Date('2028-01-01 10:00:00'));
		expect(nextMlkDay.formattedDate).toBe('2028-01-17');
	});

	it('should return Jan 20th 2029 for MLK Day if the current date is Jan 1st 2029', () => {
		const nextMlkDay = getNextMlkDay(new Date('2029-01-01 10:00:00'));
		expect(nextMlkDay.formattedDate).toBe('2029-01-15');
	});
});