import { Timespan } from "./gracc";

export function formatNumber(n: number): string {
  const roundedNumber = Math.round(n / 1000) * 1000;

  return roundedNumber.toLocaleString("en-US");
}

export function formatDate(date: Date, timespan: Timespan): string {
    switch (timespan) {
      case "daily":
        return date.toLocaleString("en-US", {
          month: "numeric",
          day: "numeric",
          hour: "numeric",
          minute: "numeric",
        });
      case "monthly":
        return date.toLocaleString("en-US", {
          month: "numeric",
          day: "numeric",
          year: "numeric",
        });
      case "yearly":
        return date.toLocaleString("en-US", {
          month: "numeric",
          day: "numeric",
          year: "numeric",
        });
    }
  }