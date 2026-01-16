import axiosApi from "../../config/axios";
import { IExpenseBodyFilters, IGetExpensesPromise } from "../../types/expense";
import { expenseByFiltersEndpoint } from "./expense.endpoints";
import { expenseFilterParsed } from "./utils";

export const findExpenseByFilters = async ({
  expenseData,
  skip,
  take,
}: IExpenseBodyFilters): Promise<IGetExpensesPromise> => {
  const { data } = await axiosApi.post<IGetExpensesPromise>(
    expenseByFiltersEndpoint(),
    expenseFilterParsed({ expenseData, skip, take })
  );

  return data;
};