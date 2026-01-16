import { DateTime, DurationObjectUnits } from "luxon";
import i18n from "i18next";

/**
 * Returns a fallback date (start of next timeframe) if no default date is provided.
 *
 * We're using a function here to allow for easier and consistent testing (mocking).
 * @returns start of next timeframe in DateTime format
 */
export const getFallbackDate = (jump: keyof DurationObjectUnits): DateTime =>
  DateTime.fromJSDate(new Date(Date.now()))
    .startOf(jump)
    .plus({ [`${jump}s`]: 1 });

/**
 * Creates a title from start time and timeframe lenght.
 * @param startDate start date of current timeframe
 * @param jump timeframe length
 * @returns string to be used as title of the component
 */
export const createDateTitle = (
  startDate: DateTime,
  jump: keyof DurationObjectUnits
): string => {
  switch (jump) {
    // for monthly view we're using the full month string
    case "month":
      return i18n.t("DateNavigationBar.Month", {
        date: startDate,
      });

    // for day view we're using the specialized day string format
    case "day":
      return i18n.t("DateNavigationBar.Day", { date: startDate });

    // for week and other views we're using standardized format: showing first and last date within the timeframe
    default:
      const jumpKey = `${jump}s`;

      const startDateString = i18n.t("DateNavigationBar.Week", {
        date: startDate,
      });
      const endDateString = i18n.t("DateNavigationBar.Week", {
        date: startDate.plus({ [jumpKey]: 1, days: -1 }),
      });

      return [startDateString, endDateString].join(" - ");
  }
};

/**
 * Helper function used to safely process date recieved from param.
 * We're using this to return parsed DateTime if input `dateISO` exists and is parsable.
 * @param dateISO string or undefined: date received from param for parsing
 * @returns
 * - parsed date in DateTime format
 * - `undefined` if date not passed or not parsable
 */
export const processDateParam = (dateISO?: string): DateTime | undefined => {
  // fail early if dateISO undefined
  if (!dateISO) return undefined;

  // try and parse the date
  const parsedDate = DateTime.fromISO(dateISO);

  // return undefined if the date was not parsed correctly
  return !parsedDate.invalidReason ? parsedDate : undefined;
};