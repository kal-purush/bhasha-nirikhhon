import moment from "moment";
import { BalanceHistoryType } from "@services/balanceHistory";

export const amountFormatter = (amount: number) => {
  const sign = amount > 0 ? "+" : amount < 0 ? "-" : "";
  const formattedAmount = `Rp ${amount.toLocaleString().replace(/,/g, ".")}`;

  return `${sign} ${formattedAmount}`;
};

export function dateFormatter(date: string) {
  const today = moment().startOf("day");
  const yesterday = moment().subtract(1, "days").startOf("day");

  if (moment(date).isSame(today, "day")) {
    return `Today at ${moment(date).format("hh:mm:ss A")}`;
  } else if (moment(date).isSame(yesterday, "day")) {
    return `Yesterday at ${moment(date).format("hh:mm:ss A")}`;
  } else {
    return moment(date).format("MMM DD, YYYY [at] hh:mm:ss A");
  }
}

export const balanceHistoryFormatter = (balanceHistory: BalanceHistoryType) => {
  const formattedDate = dateFormatter(balanceHistory.created_at);
  return {
    ...balanceHistory,
    prev_balance: balanceHistory.prev_balance ?? 0,
    user: {
      name: balanceHistory.user.name ?? "Anonymous",
      npm: balanceHistory.user.npm ?? "000000000",
    },
    created_at: formattedDate,
  };
};