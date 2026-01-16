import moment from "moment";

import {
  getEnergyUsageData,
  getDaysUntilMonthEnd,
  getDiffInDays,
  isEndOfMonth
} from "../helper";
import meterReadingsSample from "../../data/meterReadingsSample.json";

describe("Helper Functions getEnergyUsageData", () => {
  it("should return a not empty array", () => {
    const energyUsageData = getEnergyUsageData(meterReadingsSample.electricity);
    expect(energyUsageData.length).not.toBe(0);
  });

  it("should return an empty array", () => {
    const energyUsageData = getEnergyUsageData([]);
    expect(energyUsageData.length).toBe(0);
  });
});

describe("Helper Functions getDaysUntilMonthEnd", () => {
  it("should return the days until month end", () => {
    const daysUntilMonthEnd = getDaysUntilMonthEnd("2017-03-28T00:00:00.000Z");
    expect(daysUntilMonthEnd).toEqual(3);
  });
});

describe("Helper Functions getDiffInDays", () => {
  it("should return the diffin days", () => {
    const diff = getDiffInDays(
      moment("2017-04-15T00:00:00.000Z"),
      moment("2017-03-28T00:00:00.000Z")
    );
    expect(diff).toEqual(18);
  });
});

describe("Helper Functions isEndOfMonth", () => {
  it("should return is end of month", () => {
    expect(isEndOfMonth("2017-04-30T00:00:00.000Z")).toBeTruthy();
  });
  it("should return is not end of month", () => {
    expect(isEndOfMonth("2017-04-19T00:00:00.000Z")).toBeFalsy();
  });
});