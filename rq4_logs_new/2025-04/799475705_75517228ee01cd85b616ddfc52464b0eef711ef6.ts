import { isT1Zone, isT2Zone } from '@common/utils';

describe('date utils', () => {
  jest.useFakeTimers();

  describe('isT1Zone', () => {
    const zonedHours = [
      { hour: 0, result: false },
      { hour: 1, result: false },
      { hour: 2, result: false },
      { hour: 3, result: false },
      { hour: 4, result: false },
      { hour: 5, result: false },
      { hour: 6, result: false },
      { hour: 7, result: true },
      { hour: 8, result: true },
      { hour: 9, result: true },
      { hour: 10, result: true },
      { hour: 11, result: true },
      { hour: 12, result: true },
      { hour: 13, result: true },
      { hour: 14, result: true },
      { hour: 15, result: true },
      { hour: 16, result: true },
      { hour: 17, result: true },
      { hour: 18, result: true },
      { hour: 19, result: true },
      { hour: 20, result: true },
      { hour: 21, result: true },
      { hour: 22, result: true },
      { hour: 23, result: false },
    ];

    it.each(zonedHours)('should return $result for $hour hour', (payload) => {
      const date = new Date();
      date.setHours(payload.hour, 0, 0, 0);

      jest.setSystemTime(date.getTime());

      const result = isT1Zone();

      expect(result).toBe(payload.result);
    });
  });

  describe('isT2Zone', () => {
    const zonedHours = [
      { hour: 0, result: true },
      { hour: 1, result: true },
      { hour: 2, result: true },
      { hour: 3, result: true },
      { hour: 4, result: true },
      { hour: 5, result: true },
      { hour: 6, result: true },
      { hour: 7, result: false },
      { hour: 8, result: false },
      { hour: 9, result: false },
      { hour: 10, result: false },
      { hour: 11, result: false },
      { hour: 12, result: false },
      { hour: 13, result: false },
      { hour: 14, result: false },
      { hour: 15, result: false },
      { hour: 16, result: false },
      { hour: 17, result: false },
      { hour: 18, result: false },
      { hour: 19, result: false },
      { hour: 20, result: false },
      { hour: 21, result: false },
      { hour: 22, result: false },
      { hour: 23, result: true },
    ];

    it.each(zonedHours)('should return $result for $hour hour', (payload) => {
      const date = new Date();
      date.setHours(payload.hour, 0, 0, 0);

      jest.setSystemTime(date.getTime());

      const result = isT2Zone();

      expect(result).toBe(payload.result);
    });
  });
});