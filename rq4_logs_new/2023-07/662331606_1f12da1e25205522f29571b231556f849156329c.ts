import {PaginatorPage} from "./paginator-page";
import {max} from "rxjs";

export class PaginatorPageInfoHandler {
  static getCurrentPage(
    skippedItems: number,
    itemsPerPage: number
  ): number {
    return Math.ceil(skippedItems / itemsPerPage) + 1
  }

  public static buildPageInfos(
    currentPage: number,
    totalPages: number,
    maxPages: number = 5
  ): PaginatorPage[] {
    let pages: PaginatorPage[] = [];
    console.log(123);

    let amountOfPagesToShow: number = totalPages - currentPage;
    if (amountOfPagesToShow > maxPages) {
      amountOfPagesToShow = maxPages;
    }
    else if (amountOfPagesToShow < maxPages) {
      if (totalPages >= maxPages) {
        amountOfPagesToShow = maxPages;
      }
      else {
        amountOfPagesToShow = totalPages;
      }
    }

    let startPage: number = Math.ceil(currentPage - amountOfPagesToShow / 2);
    if (startPage < 1) {
      startPage = 1;
    }
    if (startPage + amountOfPagesToShow > totalPages) {
      startPage -= startPage + amountOfPagesToShow - totalPages - 1;
    }

    for (let i = 0; i < amountOfPagesToShow; i++) {
      let pageNumber: number = i + startPage;

      let page: PaginatorPage = new PaginatorPage(
        pageNumber,
        pageNumber == currentPage
      );
      pages.push(page);
    }

    return pages;
  }
}