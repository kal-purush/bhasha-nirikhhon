import { TableColumn } from "@/components/shared/table/table-models";
import { tableSortDirection, TableSortDirection } from "@/components/shared/table/table-enums";
import { VoidFunc } from "@/types/getter-setter-functions";
import { TableConfig } from "@/components/shared/table/table-config";

export function initDefaultSort(
    columns: TableColumn[],
    setSortKey: VoidFunc<string>,
    setSortDirection: VoidFunc<TableSortDirection>
) {
    const defaultSortColumn = findDefaultSortColumn(columns);

    if (defaultSortColumn) {
        setSortKey(defaultSortColumn.key);
        setSortDirection(defaultSortColumn.sort!.defaultDirection);
    }
}

export function tableSort(
    columnKey: string,
    sortKey: string | null,
    setSortKey: VoidFunc<string>,
    setSortDirection: VoidFunc<TableSortDirection>,
    columns: TableColumn[],
    currentSortDirection: TableSortDirection
) {
    if (sortKey === columnKey) {
        const newSortDirection = isSortDirectionAsc(currentSortDirection)
            ? tableSortDirection.desc
            : tableSortDirection.asc;

        setSortDirection(newSortDirection);
    } else {
        const column = columns.find((col) => col.key === columnKey);

        setSortKey(columnKey);
        setSortDirection(column?.sort?.defaultDirection ?? tableSortDirection.asc);
    }
}

export function getSortedTableRows(
    sortKey: string | null,
    tableConfig: TableConfig,
    sortDirection: TableSortDirection
) {
    const rows = [...tableConfig.rows];

    if (!sortKey) return rows;

    const sortColumn = tableConfig.columns.find((column) => column.key === sortKey);

    if (!sortColumn || !sortColumn.sort) return rows;

    const sortFn = isSortDirectionAsc(sortDirection) ? sortColumn.sort.sortByAsc : sortColumn.sort.sortByDesc;

    return rows.sort(sortFn);
}

function isSortDirectionAsc(direction: TableSortDirection) {
    return direction === tableSortDirection.asc;
}

function findDefaultSortColumn(columns: TableColumn[]) {
    return columns.find((column) => column.sort?.defaultDirection !== undefined);
}