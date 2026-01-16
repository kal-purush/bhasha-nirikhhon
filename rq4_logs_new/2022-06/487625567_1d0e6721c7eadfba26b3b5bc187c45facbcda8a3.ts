import { Workday, Workweeks } from '../../types/schedules';

export const getWorkweeks = (): Workweeks => ({
  1: Array(5).fill(null).map(getWorkday),
  2: Array(5).fill(null).map(getWorkday),
  3: Array(5).fill(null).map(getWorkday),
  4: Array(5).fill(null).map(getWorkday),
  5: Array(5).fill(null).map(getWorkday),
  6: Array(5).fill(null).map(getWorkday),
  7: Array(5).fill(null).map(getWorkday),
});

const getWorkday = (): Workday => ({
  start: [8 * 60],
  workTime: 8 * 60,
});