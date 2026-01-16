export function getTotalNumberOfPagesIntoPagination(total: number, limit: number, perPage: number) {
    if(limit === 0) {
        return Math.ceil(total / perPage);
    }
    return Math.ceil(total / limit);
}

export function getSkipIntoPagination(current_page: number, per_page: number, pages: number, total: number) {
    if (current_page === 1) {
        return 0;
    }

    if (current_page === 2) {
        return per_page;
    }

    if (current_page === pages) {
        return total;
    }

    return current_page * per_page;
}