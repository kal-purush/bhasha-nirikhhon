import {Duration} from "@prisma/client";
import {generateDatesBetween, generateDatesFromBooking, getLastDateOfBooking} from "@/app/_lib/util";
import {describe, expect, test} from "@jest/globals";
import {debug} from "console";
import {BookingsIncludeAll} from "@/app/(internal)/bookings/booking-action";

type RecursivePartial<T> = {
  [P in keyof T]?: RecursivePartial<T[P]>;
};

describe("test util functions", () => {
  describe("test getLastDateOfBooking", () => {
    const defaultDuration: Duration = {
      id: 0,
      duration: "",
      day_count: null,
      month_count: null,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    describe("[loop] check in date is 1 Jan 2024", () => {
      const checkInDate = new Date(2024, 0, 1);

      for (let i = 1; i < 25; i++) {
        test(`expect last date is correct for month: ${i}`, () => {
          const duration = {
            ...defaultDuration,
            month_count: i
          };
          const date = getLastDateOfBooking(checkInDate, duration);

          expect(date)
            .toEqual(new Date(2024, i, 0));
        });
      }


    });


    describe("[loop] check in date is 5 Jan 2024", () => {
      const checkInDate = new Date(2024, 0, 5);

      for (let i = 1; i < 25; i++) {
        test(`expect last date is correct for month: ${i}`, () => {
          const duration = {
            ...defaultDuration,
            month_count: i
          };
          const date = getLastDateOfBooking(checkInDate, duration);
          const expectedDate = new Date(2024, i + 1, 0);
          expect(date)
            .toEqual(expectedDate);
          debug(`Duration: ${i}, Check in Date: ${checkInDate.toLocaleDateString()}, Checkout Date: ${expectedDate.toLocaleDateString()}`);
        });
      }


    });

    describe("manual test", () => {
      describe("first date is 1", () => {
        const checkInDate = new Date(2024, 0, 1);
        test("1 month", () => {
          const duration = {
            ...defaultDuration,
            month_count: 1
          };

          const date = getLastDateOfBooking(checkInDate, duration);
          const expectedDate = new Date(2024, 1, 0);
          expect(date)
            .toEqual(expectedDate);
        });

        test("3 month", () => {
          const duration = {
            ...defaultDuration,
            month_count: 3
          };

          const date = getLastDateOfBooking(checkInDate, duration);
          const expectedDate = new Date(2024, 3, 0);
          expect(date)
            .toEqual(expectedDate);
        });

        test("12 month", () => {
          const duration = {
            ...defaultDuration,
            month_count: 12
          };

          const date = getLastDateOfBooking(checkInDate, duration);
          const expectedDate = new Date(2025, 0, 0);
          expect(date)
            .toEqual(expectedDate);
        });
      });

      describe("first date is not 1", () => {
        const checkInDate = new Date(2024, 0, 5);
        test("1 month", () => {
          const duration = {
            ...defaultDuration,
            month_count: 1
          };

          const date = getLastDateOfBooking(checkInDate, duration);
          const expectedDate = new Date(2024, 2, 0);
          expect(date)
            .toEqual(expectedDate);
        });

        test("3 month", () => {
          const duration = {
            ...defaultDuration,
            month_count: 3
          };

          const date = getLastDateOfBooking(checkInDate, duration);
          const expectedDate = new Date(2024, 4, 0);
          expect(date)
            .toEqual(expectedDate);
        });

        test("12 month", () => {
          const duration = {
            ...defaultDuration,
            month_count: 12
          };

          const date = getLastDateOfBooking(checkInDate, duration);
          const expectedDate = new Date(2025, 1, 0);
          expect(date)
            .toEqual(expectedDate);
        });
      });

    });
  });

  describe("test generateDatesBetween", () => {
    const startDate = new Date(2024, 0, 1);

    for (let i = 1; i < 75; i++) {
      test("should return dates correctly", () => {
        const dates = generateDatesBetween(startDate, new Date(2024, 0, i));

        expect(dates)
          .toHaveLength(i);

        expect(dates[dates.length - 1])
          .toEqual(new Date(2024, 0, i));
      });
    }
  });

  describe("test generateDatesFromBooking", () => {
    describe("start date is 1", () => {
      const defaultBookings: RecursivePartial<BookingsIncludeAll> = {
        start_date: new Date(2024, 0, 1)
      };

      test("1 month", () => {
        const booking: RecursivePartial<BookingsIncludeAll> = {
          ...defaultBookings,
          durations: {
            month_count: 1
          }
        };

        // @ts-expect-error
        const dates = generateDatesFromBooking(booking);

        expect(dates)
          .not.toBeNull();

        // @ts-ignore
        const diff = new Date(booking.start_date!).getTime() - new Date(2024, 1, 1).getTime();
        expect(dates)
          .toHaveLength(Math.abs(Math.round(diff / (1000 * 3600 * 24))));
      });

      test("3 month", () => {
        const booking: RecursivePartial<BookingsIncludeAll> = {
          ...defaultBookings,
          durations: {
            month_count: 3
          }
        };

        // @ts-expect-error
        const dates = generateDatesFromBooking(booking);

        expect(dates)
          .not.toBeNull();

        // @ts-ignore
        const diff = new Date(booking.start_date!).getTime() - new Date(2024, 3, 1).getTime();
        expect(dates)
          .toHaveLength(Math.abs(Math.round(diff / (1000 * 3600 * 24))));
      });

      test("12 month", () => {
        const booking: RecursivePartial<BookingsIncludeAll> = {
          ...defaultBookings,
          durations: {
            month_count: 12
          }
        };

        // @ts-expect-error
        const dates = generateDatesFromBooking(booking);

        expect(dates)
          .not.toBeNull();

        // @ts-ignore
        const diff = new Date(booking.start_date!).getTime() - new Date(2025, 0, 1).getTime();
        expect(dates)
          .toHaveLength(Math.abs(Math.round(diff / (1000 * 3600 * 24))));
      });

    });

    describe("start date is not 1", () => {
      const defaultBookings: RecursivePartial<BookingsIncludeAll> = {
        start_date: new Date(2024, 0, 5)
      };

      test("1 month", () => {
        const booking: RecursivePartial<BookingsIncludeAll> = {
          ...defaultBookings,
          durations: {
            month_count: 1
          }
        };

        // @ts-expect-error
        const dates = generateDatesFromBooking(booking);

        expect(dates)
          .not.toBeNull();

        // @ts-ignore
        const diff = new Date(booking.start_date!).getTime() - new Date(2024, 1, 1).getTime();

        expect(dates?.length)
          .toBeGreaterThan(Math.abs(Math.round(diff / (1000 * 3600 * 24))));
      });

      test("3 month", () => {
        const booking: RecursivePartial<BookingsIncludeAll> = {
          ...defaultBookings,
          durations: {
            month_count: 3
          }
        };

        // @ts-expect-error
        const dates = generateDatesFromBooking(booking);

        expect(dates)
          .not.toBeNull();

        // @ts-ignore
        const diff = new Date(booking.start_date!).getTime() - new Date(2024, 3, 1).getTime();
        expect(dates?.length)
          .toBeGreaterThan(Math.abs(Math.round(diff / (1000 * 3600 * 24))));
      });

      test("12 month", () => {
        const booking: RecursivePartial<BookingsIncludeAll> = {
          ...defaultBookings,
          durations: {
            month_count: 12
          }
        };

        // @ts-expect-error
        const dates = generateDatesFromBooking(booking);

        expect(dates)
          .not.toBeNull();

        // @ts-ignore
        const diff = new Date(booking.start_date!).getTime() - new Date(2025, 0, 1).getTime();
        expect(dates?.length)
          .toBeGreaterThan(Math.abs(Math.round(diff / (1000 * 3600 * 24))));
      });

    });
  });

});