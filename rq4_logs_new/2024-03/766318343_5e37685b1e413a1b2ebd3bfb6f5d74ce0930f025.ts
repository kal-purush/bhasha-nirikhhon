import { usePathname, useSearchParams } from "next/navigation";

export const ELLIPSIS_LEFT = -1;
export const ELLIPSIS_RIGHT = -2;

export const usePagination = (currentPage: number, totalPages: number) => {
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const generatePages = () => {
    if (totalPages <= 7) {
      return Array.from({ length: totalPages }, (_, i) => i + 1);
    }

    if (currentPage < 3) {
      return [1, 2, 3, ELLIPSIS_LEFT, totalPages];
    }

    if (currentPage > totalPages - 2) {
      return [
        1,
        2,
        3,
        ELLIPSIS_LEFT,
        totalPages - 2,
        totalPages - 1,
        totalPages,
      ];
    }

    return [
      1,
      ELLIPSIS_LEFT,
      currentPage - 1,
      currentPage,
      currentPage + 1,
      ELLIPSIS_RIGHT,
      totalPages,
    ];
  };

  const pages = generatePages().map((page) => {
    const params = new URLSearchParams(searchParams);
    params.set("page", page.toString());
    const url = `${pathname}?${params.toString()}`;
    const isCurrentPage = page === currentPage;
    const isEllipsis = page === ELLIPSIS_LEFT || page === ELLIPSIS_RIGHT;

    return {
      pageNumber: page,
      url,
      isCurrentPage,
      isEllipsis,
    };
  });

  return pages;
};