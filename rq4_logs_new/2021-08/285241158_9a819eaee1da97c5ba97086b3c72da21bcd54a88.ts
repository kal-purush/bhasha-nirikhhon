import { Category, SlotType } from "eisbuk-shared";

import { LocalStore } from "@/types/store";

import { dummySlot } from "@/__testData__/dummyData";

/**
 * A week we're providing to test filtering out dates before requested time frame in `getSlotsForCustomer`.
 */
const prevWeek = {
  "2021-02-22": {
    ["slot-0"]: {
      ...dummySlot,
      categories: [Category.Competitive],
      id: "slot-0",
      type: SlotType.Ice,
    },
  },
  "2021-02-23": {
    ["slot-1"]: {
      ...dummySlot,
      categories: [Category.PreCompetitive],
      id: "slot-1",
      type: SlotType.OffIceDancing,
    },
  },
  "2021-02-24": {
    ["slot-2"]: {
      ...dummySlot,
      categories: [Category.Course],
      id: "slot-2",
      type: SlotType.OffIceGym,
    },
  },
};

/**
 * Slots of currentWeek (starting with "2021-03-01"), belonging to the `Category.Competitive`.
 * We're injecting this record into the `currentWeek`
 * (containing slots of current week which don't belong with `Category.Competitive`).
 * When testing for `getSlotsForCustomer` with `category:Category.Competitive` and `timeframe: "week"` we'll be expecting this record.
 */
export const currentWeekCompetitive = {
  "2021-03-01": {
    ["slot-3"]: {
      ...dummySlot,
      id: "slot-3",
      categories: [Category.Competitive, Category.PreCompetitive],
      type: SlotType.Ice,
    },
  },
  "2021-03-03": {
    ["slot-4"]: {
      ...dummySlot,
      id: "slot-4",
      categories: [Category.Competitive],
      type: SlotType.OffIceGym,
    },
  },
};

/**
 * Slots of nextWeek (of the week starting with "2021-03-01"), belonging to the `Category.Competitive`.
 * We're injecting this record into the `nextWeek` as well as `currentMonthCompetitive`.
 */
const nextWeekCompetitive = {
  "2021-03-08": {
    ["slot-5"]: {
      ...dummySlot,
      id: "slot-5",
      categories: [Category.Competitive, Category.PreCompetitive],
      type: SlotType.Ice,
    },
  },
  "2021-03-09": {
    ["slot-6"]: {
      ...dummySlot,
      id: "slot-6",
      categories: [Category.Competitive],
      type: SlotType.OffIceGym,
    },
  },
};

/**
 * A current week of slots we're using as test data. Contains both slots
 * in compliance with `Category.Competitive` (which we're expecting when testing the `getSlotsForCustomer`)
 * as well as those of different categories than `Category.Competitive` (in test, we'll be expecting those to be filtered out).
 */
const currentWeek = {
  "2021-03-01": {
    ["slot-7"]: {
      ...dummySlot,
      id: "slot-7",
      categories: [Category.PreCompetitive],
      type: SlotType.Ice,
    },
    ...currentWeekCompetitive["2021-03-01"],
  },
  "2021-03-02": {
    ["slot-8"]: {
      ...dummySlot,
      id: "slot-8",
      categories: [Category.PreCompetitive],
      type: SlotType.OffIceDancing,
    },
  },
  "2021-03-03": {
    ["slot-9"]: {
      ...dummySlot,
      id: "slot-9",
      categories: [Category.PreCompetitive],
      type: SlotType.OffIceGym,
    },
    ...currentWeekCompetitive["2021-03-03"],
  },
};

/**
 * A week we're providing to both have a bit more content of the `currentMonth`
 * and have some data we want to make sure to filter out when testing `getSlotsForCustomer` for `"week"` timeframe.
 */
const nextWeek = {
  "2021-03-08": {
    ["slot-10"]: {
      ...dummySlot,
      id: "slot-10",
      categories: [Category.PreCompetitive],
      type: SlotType.Ice,
    },
    ...nextWeekCompetitive["2021-03-08"],
  },
  "2021-03-09": {
    ["slot-11"]: {
      ...dummySlot,
      id: "slot-11",
      categories: [Category.PreCompetitive],
      type: SlotType.OffIceDancing,
    },
    ...nextWeekCompetitive["2021-03-09"],
  },
  "2021-03-10": {
    ["slot-12"]: {
      ...dummySlot,
      id: "slot-12",
      categories: [Category.PreCompetitive],
      type: SlotType.OffIceGym,
    },
  },
};

/**
 * A month we're providing to test filtering out dates before requested time frame in `getSlotsForCustomer`.
 */
const prevMonth = { ...prevWeek };

/**
 * A current month of slots we're using for tests.
 * Contains both the slots in compliance with `Category.Competitive` (which we're expecting when testing the `getSlotsForCustomer`)
 * as well as those of different categories than `Category.Competitive` (in test, we'll be expecting those to be filtered out).
 */
export const currentMonthCompetitive = {
  ...currentWeekCompetitive,
  ...nextWeekCompetitive,
};

/**
 * A month we're providing to test filtering out dates after requested time frame in `getSlotsForCustomer`.
 */
const nextMonth = {
  "2021-04-01": {
    ["slot-13"]: {
      ...dummySlot,
      id: "slot-13",
      categories: [Category.Competitive],
      type: SlotType.OffIceDancing,
    },
  },
  "2021-04-09": {
    ["slot-14"]: {
      ...dummySlot,
      id: "slot-14",
      categories: [Category.PreCompetitive],
      type: SlotType.OffIceGym,
    },
  },
  "2021-04-10": {
    ["slot-15"]: {
      ...dummySlot,
      id: "slot-15",
      categories: [Category.Competitive, Category.PreCompetitive],
      type: SlotType.Ice,
    },
  },
};

/**
 * Dummy slots by day we're using to test selecting slot data from store
 */
const slotsByDay: LocalStore["firestore"]["data"]["slotsByDay"] = {
  ["2021-02"]: prevMonth,
  ["2021-03"]: { ...currentWeek, ...nextWeek },
  ["2021-04"]: nextMonth,
};

/**
 * An intermediate step (used for easier type casting).
 * `firestore` entry in store, populated with our dummy slot data.
 */
const firestore = {
  data: {
    slotsByDay,
  },
} as LocalStore["firestore"];

/**
 * Partial local store populated with our test slots (within `firestore.data.slotsByDay`).
 */
export const testStoreState: Partial<LocalStore> = {
  firestore,
};