import { QueryDslQueryContainer } from '@elastic/elasticsearch/lib/api/types';
import { DateTime } from 'luxon';

import { Timespan } from '@weco/content-api/src/controllers/events';

// TODO
// review lt and gt vs lte and gte
// review where should use end date vs start date
// review the sorting order, it's opposite from what's on?
// TODO what do we do with TODAY when it's a Monday? Events with ranges will display results
export const getTimespanQuery = (
  timespan: Timespan
): QueryDslQueryContainer | undefined => {
  const now = DateTime.local({ zone: 'Europe/London' });

  switch (timespan) {
    // TODO https://github.com/wellcomecollection/content-api/issues/130#issuecomment-2656772912
    case 'today':
      return {
        range: {
          // TODO change as it should be filter values and maybe end date
          'query.times.startDateTime': {
            gte: 'now',
            lte: now.endOf('day').toISO(),
          },
        },
      };

    // Maybe we want to pass in a different relation here? within or contain?
    case 'this-weekend':
      console.log(now.startOf('week').plus({ days: 6 }).endOf('day'));
      return {
        range: {
          // TODO change as it should be filter values and maybe end date
          'query.times.startDateTime': {
            gte: now.startOf('week').plus({ days: 5 }),
            lte: now.startOf('week').plus({ days: 6 }).endOf('day'),
          },
        },
      };

    case 'this-week':
      return {
        range: {
          // TODO change as it should be filter values and maybe end date
          'query.times.startDateTime': {
            gte: 'now',
            lte: now.endOf('week').toISO(),
          },
        },
      };

    case 'this-month':
      return {
        range: {
          // TODO change as it should be filter values and maybe end date
          'query.times.startDateTime': {
            gte: 'now',
            lte: now.endOf('month').toISO(),
          },
        },
      };

    case 'future':
      return {
        range: {
          // TODO change as it should be filter values and maybe end date
          'query.times.startDateTime': {
            gte: 'now',
          },
        },
      };

    case 'past':
      return {
        range: {
          // TODO change as it should be filter values and maybe end date
          'query.times.startDateTime': {
            lt: 'now',
          },
        },
      };

    case 'january':
    case 'february':
    case 'march':
    case 'april':
    case 'may':
    case 'june':
    case 'july':
    case 'august':
    case 'september':
    case 'october':
    case 'november':
    case 'december': {
      // TODO this is a bit workaround-y, might be best to just go with an array
      const monthNumber = DateTime.fromFormat(
        `${timespan} 01, 2001`,
        'MMMM dd, yyyy'
      ).month;
      const startOfMonth = DateTime.local(now.year, monthNumber);
      const endOfMonth = startOfMonth.endOf('month');

      return {
        range: {
          // TODO change as it should be filter values and maybe end date
          'query.times.startDateTime': {
            gte: startOfMonth,
            lte: endOfMonth,
          },
        },
      };
    }
  }
};