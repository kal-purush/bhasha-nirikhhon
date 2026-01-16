import { PaginatedContext } from './paginated-context';

export interface OnlinePaginatedContext extends PaginatedContext {
    lastIdInPage?: string;
    lastDataVersionInPage?: number;
    isLastPage?: boolean;
}