// #docregion
import { Filter } from './filter';

export const FILTERS: Filter[] = [
  {
    id: 0,
    name: 'type',
    isCalendar: false,
    defaultValue: -1,
    options: [
      { name: 'Show all tasks', value: -1 },
      { name: 'Show delegates tasks only', value: 0 },
      { name: 'Show issues only', value: 1 },
    ],
  },
  {
    id: 1,
    name: 'status',
    isCalendar: false,
    defaultValue: -1,
    options: [
      { name: 'Filter by Status', value: -1 },
      { name: 'High', value: 0 },
      { name: 'Normal', value: 1 },
      { name: 'Low', value: 2 },
    ],
  }
];