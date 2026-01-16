import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { describe, expect, it } from 'vitest';

import { dateFormat, dateTimeFormat, timeFormat } from './dateFormat';

dayjs.extend(utc);

describe('dateFormat', () => {
  const dateTime = '2017-05-02T01:54:25.455';

  describe('dateFormat', () => {
    it('skal formatere dato med default format', () => {
      expect(dateFormat(dateTime)).toEqual('02.05.2017');
    });
    it('skal formatere dato med tilpasset format', () => {
      expect(
        dateFormat(dateTime, {
          day: 'numeric',
          month: 'long',
          year: '2-digit',
        }),
      ).toEqual('2. mai 17');
    });
  });

  describe('dateTimeFormat', () => {
    it('skal formatere en dato med default format', () => {
      expect(dateTimeFormat(dateTime)).toEqual('02.05.2017 - 01:54');
    });
    it('skal formatere datetime med tilpasset format', async () => {
      expect(
        dateTimeFormat(dateTime, {
          separator: 'kl',
          day: 'numeric',
          month: 'long',
          year: '2-digit',
          second: '2-digit',
        }),
      ).toEqual('2. mai 17 kl. 01:54:25');
    });
  });

  describe('timeFormat', () => {
    it('skal formatere datetime til å vise kun klokkeslett', () => {
      expect(timeFormat(dateTime)).toEqual('01:54');
    });
  });
});