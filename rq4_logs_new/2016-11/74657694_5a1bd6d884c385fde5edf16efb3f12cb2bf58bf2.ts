import { Injectable } from '@angular/core';
import * as _ from 'underscore';

const MAX_ITEMS_PER_PAGE = 10;
const MAX_ITEMS_HALF = MAX_ITEMS_PER_PAGE / 2;

@Injectable()
export class PaginationService {
    constructor() {}

    private createPagesArray(startPage, endPage): Array<number> {
        return _.range(startPage, endPage + 1);
    }

    private calculateTotalPages(totalItems: number, pageSize: number) {
        return Math.ceil(totalItems / pageSize);
    }

    private calculateBorderIndexes(currentPage: number, pageSize: number, totalItems: number): Array<number> {
        let startIndex = (currentPage - 1) * pageSize;
        let endIndex = Math.min(startIndex + pageSize - 1, totalItems - 1);
        return [startIndex, endIndex];
    }

    private calculatePagesForMultiplePages(currentPage, startPage: any, endPage: any, totalPages): Array<number> {
        let isCurrentPageInFirstPaginationHalf = currentPage <= MAX_ITEMS_HALF + 1;
        let isCurrentPageInLastPaginationHalf = currentPage + (MAX_ITEMS_HALF - 1) >= totalPages;

        switch (true) {
            case isCurrentPageInFirstPaginationHalf:
                [startPage, endPage] = [1, MAX_ITEMS_PER_PAGE];
                break;

            case isCurrentPageInLastPaginationHalf:
                [startPage, endPage] = [totalPages - (MAX_ITEMS_PER_PAGE - 1), totalPages];
                break;

            default:
                [startPage, endPage] = [currentPage - MAX_ITEMS_HALF, currentPage + (MAX_ITEMS_HALF - 1)];
        }

        return [startPage, endPage];
    }

    private calculateBorderPages(totalPages, currentPage): Array<number> {
        let startPage = null;
        let endPage = null;

        if (totalPages <= MAX_ITEMS_PER_PAGE) {
            [startPage, endPage] = this.calculatePagesForSinglePage(startPage, endPage, totalPages);
        } else {
            [startPage, endPage] = this.calculatePagesForMultiplePages(currentPage, startPage, endPage, totalPages);
        }

        return [startPage, endPage];
    }

    private calculatePagesForSinglePage(startPage: any, endPage: any, totalPages): Array<number> {
        startPage = 1;
        endPage = totalPages;
        return [startPage, endPage];
    }

    getPagination(totalItems: number, currentPage: number = 1, pageSize: number = 10): Object {
        let totalPages = this.calculateTotalPages(totalItems, pageSize);
        let [startPage, endPage] = this.calculateBorderPages(totalPages, currentPage);
        let [startIndex, endIndex] = this.calculateBorderIndexes(currentPage, pageSize, totalItems);
        let pages = this.createPagesArray(startPage, endPage);

        return {
            totalItems,
            currentPage,
            pageSize,
            totalPages,
            startPage,
            endPage,
            startIndex,
            endIndex,
            pages
        };
    }
}