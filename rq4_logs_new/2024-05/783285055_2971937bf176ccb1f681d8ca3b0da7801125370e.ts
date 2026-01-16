'use client';

import { As } from '@/types';
import { cn } from '@/utils';
import {
  Dispatch,
  HTMLAttributes,
  SetStateAction,
  useCallback,
  useMemo,
} from 'react';

export interface UsePaginationProp extends HTMLAttributes<HTMLDivElement> {
  total: number;
  page: number;
  setPage: Dispatch<SetStateAction<number>>;

  as?: As;
  onNext?: () => void;
  onPrev?: () => void;
}

export function usePagination({ ...props }: UsePaginationProp) {
  const { page, setPage, total, onNext, onPrev, as, ...otherProps } = props;

  const pageSize = 10;
  const Component = as || 'div';

  const startPage = useMemo(
    () => Math.floor((page - 1) / pageSize) * pageSize + 1,
    [page, pageSize],
  );

  const endPage = useMemo(
    () => Math.min(startPage + pageSize - 1, total),
    [total, pageSize, startPage],
  );

  const displayedPages = useMemo(
    () =>
      Array.from({ length: endPage - startPage + 1 }, (_, i) => startPage + i),
    [startPage, endPage],
  );

  const getBaseProps = useCallback(
    () => ({
      className: cn('border-1', otherProps.className),
      ...otherProps,
    }),
    [otherProps],
  );

  const getPrevButtonProps = useCallback(
    () => ({
      onClick: () => {
        if (page > 1) {
          setPage((prev) => prev - 1);
          onPrev?.();
        }
      },
      disabled: page <= 1,
      className: cn(
        'w-25 h-25 bg-[white] border-1 border-[#DFE3E8]',
        page <= 1 && 'bg-[#DFE3E8]',
      ),
    }),
    [setPage, page, onPrev],
  );

  const getNextButtonProps = useCallback(
    () => ({
      onClick: () => {
        if (page < total) {
          setPage((prev) => prev + 1);
          onNext?.();
        }
      },
      disabled: page >= total,
      className: cn(
        'w-25 h-25 bg-[white] border-1 border-[#DFE3E8]',
        page >= total && 'bg-[#DFE3E8]',
      ),
    }),
    [setPage, page, total, onNext],
  );

  const getPageButtonProps = useCallback(
    (currentPage: number) => ({
      onClick: () => setPage(() => currentPage),
      className: cn(
        'w-25 h-25 rounded-4 border-1 border-[#DFE3E8] text-14',
        page === currentPage && 'border-[#F08D53] text-[#F08D53]',
      ),
    }),
    [page, setPage],
  );

  return {
    Component,
    page,
    setPage,
    displayedPages,
    getBaseProps,
    getPrevButtonProps,
    getNextButtonProps,
    getPageButtonProps,
  };
}

export type UsePaginationReturn = ReturnType<typeof usePagination>;