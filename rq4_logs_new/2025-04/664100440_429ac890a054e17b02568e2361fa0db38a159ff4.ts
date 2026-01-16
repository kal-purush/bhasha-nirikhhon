import { useMemo } from "react";

export const usePagination = (totalCount: number, itemsPerPage: number, currentPage: number) => {
  return useMemo(() => {
    const totalPages = Math.ceil(totalCount / itemsPerPage);
    const maxPagesToShow = 5;
    
    if (totalPages <= 1) return { pageNumbers: [], totalPages };

    let pageNumbers: (number | "...")[] = [];

    if (totalPages <= maxPagesToShow) {
      pageNumbers = Array.from({ length: totalPages }, (_, i) => i + 1);
    } else {
      if (currentPage > 3) pageNumbers.push(1, "...");

      const start = Math.max(2, currentPage - 1);
      const end = Math.min(totalPages - 1, currentPage + 1);

      for (let i = start; i <= end; i++) pageNumbers.push(i);

      if (currentPage < totalPages - 2) pageNumbers.push("...", totalPages);
    }

    return { pageNumbers, totalPages };
  }, [totalCount, itemsPerPage, currentPage]);
};