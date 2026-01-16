import { PAGINATION_LIMIT, type IPagination } from "./types";

export function createPagination(
  totalCount: number,
  page?: number,
): IPagination {
  const totalPages = Math.ceil(totalCount / PAGINATION_LIMIT);
  const currentPage = page ?? totalPages;

  if (currentPage > totalPages) {
    throw new Error(
      `Current page ${page} of total count ${totalCount} and limit ${PAGINATION_LIMIT} is greater than ${totalPages}.`,
    );
  }

  const offset = (currentPage - 1) * PAGINATION_LIMIT;
  const currentMin = offset + 1;
  const currentMax =
    offset + PAGINATION_LIMIT > totalCount
      ? totalCount
      : offset + PAGINATION_LIMIT;

  const pagination: IPagination = {
    totalCount,
    totalPages,
    currentPage,
    limit: PAGINATION_LIMIT,
    offset,
    currentMin,
    currentMax,
  };

  return pagination;
}