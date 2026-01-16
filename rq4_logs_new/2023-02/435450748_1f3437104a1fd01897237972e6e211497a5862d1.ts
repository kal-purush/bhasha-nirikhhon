export interface IResultsTablePaginationProps {
    rows: Result[];
    page: number;
    setPage: (page: number) => void;
    rowsPerPage: number;
    setRowsPerPage: (rowsPerPage: number) => void;
}

export type Result = {
    id?: string;
    username: string;
    score: number;
};