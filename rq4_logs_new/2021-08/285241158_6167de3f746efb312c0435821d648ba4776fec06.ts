import { DateTime } from "luxon";

import { orderByWeekDay } from "../utils";

import { luxon2ISODate } from "@/utils/date";

import { slotsMonth } from "@/__testData__/dummyData";

describe("CustomerSlots", () => {
  describe("Test utils: 'orderByWeekDay'", () => {
    test("should accept an array of dates and order the dates by day of the week (mondays, tuesdays, etc.)", () => {
      const testDates = Object.keys(slotsMonth);
      const orderedDates = orderByWeekDay(testDates);
      // manually reorder dates to get desired order
      // ["mon", "wed", "mon", "wed", "mon", "wed", "mon", "wed"] --> ["mon", "mon", "mon", "mon", "wed", "wed", "wed", "wed"]
      const expectedDates = [
        testDates[0],
        testDates[2],
        testDates[4],
        testDates[6],
        testDates[1],
        testDates[3],
        testDates[5],
        testDates[7],
      ];
      expect(orderedDates).toEqual(expectedDates);
    });

    test("when week dates aren't ordered, should order them internally", () => {
      // we'll be using down sorted dates for testing
      // all dates will be mondays ( as we've already tested ordering by week day)

      // our start day will be last monday in "2021-02"
      const startDate = DateTime.fromISO("2021-03-01").minus({ weeks: 1 });
      const testDates = Array(4)
        .fill(startDate)
        .map((luxonDay: DateTime, i) =>
          luxon2ISODate(luxonDay.minus({ weeks: i }))
        );

      const orderedDates = orderByWeekDay(testDates);
      // manually reorder dates to get desired order
      // ["mon-3", "mon-2", "mon-1", "mon-0"] --> ["mon-0", "mon-1", "mon-2", "mon-3"]
      const expectedDates = [
        testDates[3],
        testDates[2],
        testDates[1],
        testDates[0],
      ];
      expect(orderedDates).toEqual(expectedDates);
    });
  });
});