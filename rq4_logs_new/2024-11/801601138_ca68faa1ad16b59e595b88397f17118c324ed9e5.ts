import {
  DataTableHandle,
  DataTableRow,
} from "@/components/base/data-table/data-table";
import { ForwardedRef } from "react";

export function getColumnKey(column: any): string {
  // Type guard to check if the column has an `accessorKey` property
  if ("accessorKey" in column && typeof column.accessorKey === "string") {
    return column.accessorKey;
  }
  // Fallback to using `id` if `accessorKey` does not exist
  return column.id as string;
}

export function safeRefresh(ref: ForwardedRef<DataTableHandle>) {
  if (ref && "current" in ref && ref.current) {
    ref.current.refresh();
  }
}

export function groupDataByKey<TData extends DataTableRow<TData>>(
  data: TData[],
  groupByKey: keyof TData
): TData[] {
  const groupedData = data.reduce(
    (acc, item) => {
      const groupKey = item[groupByKey];

      if (!acc[groupKey]) {
        acc[groupKey] = {
          [groupByKey]: groupKey,
          subRows: [],
        };
      }

      acc[groupKey].subRows.push(item);
      return acc;
    },
    {} as Record<string, any>
  );

  // Convert grouped data to an array of grouped items with subRows
  return Object.values(groupedData);
}