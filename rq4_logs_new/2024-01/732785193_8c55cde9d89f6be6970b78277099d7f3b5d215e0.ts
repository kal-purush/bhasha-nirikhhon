import { test, expect } from "@playwright/test";
import { getTimestampToday } from "../../../../build/_common/lib/timeFormats.mjs";

test.describe("Format for Timestamps", () => {
  const timestamp = getTimestampToday();
  const splitTimestamp = timestamp.split(", ");
  const date = splitTimestamp[0];
  const time = splitTimestamp[1];

  test("should consist of two parts", () => {
    expect(splitTimestamp.length).toBe(2);
  });

  test("should have specific lengths for both parts", () => {
    // 01/01/2000 -> 10 characters
    expect(date.length).toBe(10);
    // 14:07:53 -> 8 characters
    expect(time.length).toBe(8);
  });

  test("should be a string", () => {
    expect(typeof timestamp).toBe("string");
  });
});