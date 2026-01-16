import { ITransaction } from '../../types';

export const paginate = (
  results: ITransaction[],
  page: number = 1,
  limit: number = 10,
) => {
  const totalCounts = results?.length;
  const start = (page - 1) * limit;
  const end = page * limit;
  const items = results.slice(start, end);
  const hasNextPage = limit * page < totalCounts;
  const hasPreviousPage = page > 1;

  return {
    totalCounts,
    itemsPerPage: +limit,
    hasPreviousPage,
    hasNextPage,
    currentPage: +page,
    results: items,
  };
};