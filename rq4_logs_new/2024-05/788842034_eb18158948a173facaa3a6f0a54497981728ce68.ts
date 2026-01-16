import { PaginationFilters } from "@/types/Pagination";
import { useCallback, useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

const useFiltersAndPagination = <T extends PaginationFilters>(
  filterKeys: (keyof T)[],
  limit: number = 10
) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [totalPages, setTotalPages] = useState(0);

  function initFilter(): T {
    // Initialize the filter with page and limit
    const newFilter: PaginationFilters = {
      page: parseInt(searchParams.get("page") as string) || 1,
      limit,
    };

    // Add additional filters based on filterKeys
    filterKeys.forEach((key) => {
      const value = searchParams.get(key as string);
      if (value !== null) {
        newFilter[key as string] = value;
      }
    });

    // Type assertion to T
    return newFilter as T;
  }

  const initialFilters = initFilter();

  const [filters, setFilters] = useState(initialFilters);

  const [apiParamsString, setApiParamsString] = useState(
    createParamsString(filters)
  );

  function createParamsString(filters: PaginationFilters): string {
    let paramsString = "?";
    const entriesParams = Object.entries(filters);
    const entriesParamsString: string[] = [];
    for (const [key, value] of entriesParams) {
      if (value) {
        const entryString = `${key}=${value}`;
        entriesParamsString.push(entryString);
      }
    }
    return (paramsString += entriesParamsString.join("&"));
  }

  function createQueryParams(filters: PaginationFilters): URLSearchParams {
    const entriesParams = Object.entries(filters);
    const newQueryParams = new URLSearchParams();
    for (const [key, value] of entriesParams) {
      if (value && key !== "limit") {
        newQueryParams.set(key, `${value}`);
      }
    }
    return newQueryParams;
  }

  const updateParams = useCallback(() => {
    setApiParamsString(createParamsString(filters));
    setSearchParams(createQueryParams(filters));
  }, [filters, setSearchParams]);

  useEffect(() => {
    updateParams();
  }, [updateParams]);

  const updateFilters = (key: string, value: string | null) => {
    setFilters((previousFilters) => ({
      ...previousFilters,
      [key]: value,
      page: 1,
    }));
  };

  const nextPage = () => {
    if (filters.page - 1 !== 0) {
      setFilters((previousFilters) => ({
        ...previousFilters,
        page: previousFilters.page - 1,
      }));
    }
  };

  const previousPage = () => {
    if (filters.page + 1 <= totalPages) {
      setFilters((previousFilters) => ({
        ...previousFilters,
        page: previousFilters.page + 1,
      }));
    }
  };

  return {
    filters,
    apiParamsString,
    totalPages,
    updateFilters,
    nextPage,
    previousPage,
    setTotalPages,
  };
};

export default useFiltersAndPagination;