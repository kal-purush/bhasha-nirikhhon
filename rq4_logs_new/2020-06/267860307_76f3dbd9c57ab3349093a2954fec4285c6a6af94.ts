import {
  getChartData,
  ChartData,
  findBirthdayNumberOne,
  BirthdayNumberOnes,
  NoDataReason,
} from "./ChartData";
import { DateTime } from "luxon";

const singleEntryOneWeek: ChartData = [
  {
    firstWeekEndDate: DateTime.fromISO("1952-11-14T00:00:00"),
    title: "HERE IN MY HEART",
    artist: "AL MARTINO",
    weeksAtNumberOne: 1,
  },
];

const singleEntryMultipleWeeks: ChartData = [
  {
    firstWeekEndDate: DateTime.fromISO("1952-11-14T00:00:00"),
    title: "HERE IN MY HEART",
    artist: "AL MARTINO",
    weeksAtNumberOne: 9,
  },
];

test("findBirthdayNumberOnes returns DATE_TOO_OLD if birthday much before earliest chart", () => {
  const muchBeforeChartStart = DateTime.fromISO("1950-01-01");
  expect(
    findBirthdayNumberOne(muchBeforeChartStart, singleEntryOneWeek)
  ).toStrictEqual({
    date: muchBeforeChartStart,
    numberOne: null,
    reason: NoDataReason.DATE_TOO_OLD,
  });
});

test("findBirthdayNumberOnes returns DATE_TOO_OLD if birthday one day before earliest chart", () => {
  const oneDayBeforeChartStart = DateTime.fromISO("1952-11-07");
  expect(
    findBirthdayNumberOne(oneDayBeforeChartStart, singleEntryOneWeek)
  ).toStrictEqual({
    date: oneDayBeforeChartStart,
    numberOne: null,
    reason: NoDataReason.DATE_TOO_OLD,
  });
});

test("findBirthdayNumberOnes returns song on first day of chart", () => {
  const firstDayOfChart = DateTime.fromISO("1952-11-08");
  expect(
    findBirthdayNumberOne(firstDayOfChart, singleEntryOneWeek)
  ).toStrictEqual({
    date: firstDayOfChart,
    numberOne: singleEntryOneWeek[0],
    reason: null,
  });
});

test("findBirthdayNumberOnes returns NO_DATA_YET after last day of chart", () => {
  const afterLastDayOfChart = DateTime.fromISO("1952-11-15");
  expect(
    findBirthdayNumberOne(afterLastDayOfChart, singleEntryOneWeek)
  ).toStrictEqual({
    date: afterLastDayOfChart,
    numberOne: null,
    reason: NoDataReason.NO_DATA_YET,
  });
});

test("findBirthdayNumberOnes returns song on last day of chart", () => {
  const lastDayOfChart = DateTime.fromISO("1952-11-14");
  expect(
    findBirthdayNumberOne(lastDayOfChart, singleEntryOneWeek)
  ).toStrictEqual({
    date: lastDayOfChart,
    numberOne: singleEntryOneWeek[0],
    reason: null,
  });
});

test("findBirthdayNumberOnes returns song on last day of chart with multiple weeks", () => {
  const lastDayOfChart = DateTime.fromISO("1953-01-09");
  expect(
    findBirthdayNumberOne(lastDayOfChart, singleEntryMultipleWeeks)
  ).toStrictEqual({
    date: lastDayOfChart,
    numberOne: singleEntryMultipleWeeks[0],
    reason: null,
  });
});

test("findBirthdayNumberOnes returns NO_DATA_YET after last day of chart with multiple weeks", () => {
  const lastDayOfChart = DateTime.fromISO("1953-01-10");
  expect(
    findBirthdayNumberOne(lastDayOfChart, singleEntryMultipleWeeks)
  ).toStrictEqual({
    date: lastDayOfChart,
    numberOne: null,
    reason: NoDataReason.NO_DATA_YET,
  });
});