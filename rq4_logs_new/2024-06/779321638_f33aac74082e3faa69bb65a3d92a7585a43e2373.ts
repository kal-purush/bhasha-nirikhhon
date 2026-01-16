import {
  ICreateExpense,
  ICreateExpenseForm,
  IExpenseFilterData,
  IExpenseFormFilter,
} from "../../../types/expense";

export const expenseFiltersParser = (
  values: IExpenseFormFilter
): IExpenseFilterData => {
  return {
    ...(values.Category && { categoryId: values.Category.value }),
    ...(values.description && { description: values.description }),
    ...(values.startDate && { startDate: values.startDate }),
    ...(values.endDate && { endDate: values.endDate }),
  };
};

export const expenseCreateParser = ({
  amount,
  description,
  Category,
}: ICreateExpenseForm): ICreateExpense => {
  return {
    amount,
    description,
    categoryId: Category.value,
  };
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const numericComparator = (rowA: any, rowB: any, columnId: string) => {
  const a = Number(rowA.getValue(columnId));
  const b = Number(rowB.getValue(columnId));

  if (a === null || a === undefined) {
    return 1;
  }
  if (b === null || b === undefined) {
    return -1;
  }
  if (a === b) {
    return 0;
  }

  return a > b ? 1 : -1;
};