import axios from "axios";
import { TransactionEntry } from "../types";

interface TransactionResponseEntry extends Omit<TransactionEntry, "date"> {
  id: string;
  date: string;
}

export const GetInitialDebitTransactions = async (): Promise<
  Array<TransactionEntry>
> => {
  const response = await axios.get("https://moj-api.herokuapp.com/debits");
  const transactions: Array<TransactionEntry> = response.data.map(
    (entry: TransactionResponseEntry) => ({
      description: entry.description,
      amount: entry.amount,
      date: new Date(entry.date),
    })
  );
  return transactions;
};

export const GetInitialCreditTransactions = async (): Promise<
  Array<TransactionEntry>
> => {
  const response = await axios.get("https://moj-api.herokuapp.com/credits");
  const transactions: Array<TransactionEntry> = response.data.map(
    (entry: TransactionResponseEntry) => ({
      description: entry.description,
      amount: entry.amount,
      date: new Date(entry.date),
    })
  );
  return transactions;
};