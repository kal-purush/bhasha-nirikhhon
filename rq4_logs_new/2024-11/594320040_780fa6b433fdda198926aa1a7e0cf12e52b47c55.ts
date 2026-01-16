import { AccountExpense, FormElementsState } from "../../../types";
import { Paths, useGet, useGetList, useMutationApi } from "../factory";

export interface AccountExpensePayload {
  data: AccountExpense[];
  totalNumber: number;
  totalPages: number;
  page: number;
  limit: number;
  generalTotalExpense: number;
}
const baseUrl = `${Paths.Accounting}/expenses`;

export function useAccountExpenseMutations() {
  const {
    deleteItem: deleteAccountExpense,
    updateItem: updateAccountExpense,
    createItem: createAccountExpense,
  } = useMutationApi<AccountExpense>({
    baseQuery: baseUrl,
  });

  return { deleteAccountExpense, updateAccountExpense, createAccountExpense };
}
export function useGetAccountProductExpenses(product: string) {
  const url = `${Paths.Accounting}/product_expense`;
  return useGetList<AccountExpense>(
    `${url}?product=${product}`,
    [url, product],
    false
  );
}

export function useGetAccountExpenses(
  page: number,
  limit: number,
  filterPanelElements: FormElementsState
) {
  return useGet<AccountExpensePayload>(
    `${Paths.Accounting}/expenses?page=${page}&limit=${limit}&product=${filterPanelElements.product}&service=${filterPanelElements.service}&type=${filterPanelElements.type}&expenseType=${filterPanelElements.expenseType}&location=${filterPanelElements.location}&brand=${filterPanelElements.brand}&vendor=${filterPanelElements.vendor}&before=${filterPanelElements.before}&after=${filterPanelElements.after}&sort=${filterPanelElements.sort}&asc=${filterPanelElements.asc}`,
    [baseUrl, page, limit, filterPanelElements],
    false
  );
}