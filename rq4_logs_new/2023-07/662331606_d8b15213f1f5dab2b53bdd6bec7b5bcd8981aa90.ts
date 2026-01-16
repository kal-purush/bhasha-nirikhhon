export class ItemsHandler {
  public static getSkipItemsNumberByPage(
    page: number,
    itemsPerPage: number
  ): number {
    return (page - 1) * itemsPerPage;
  }

  public static getTotalPagesNumber(
    totalItemsNumber: number,
    itemsPerPage: number
  ): number {
    return Math.ceil( totalItemsNumber / itemsPerPage );
  }

  static getCurrentPage(
    skippedItems: number,
    itemsPerPage: number
  ): number {
    return Math.ceil( skippedItems / itemsPerPage ) + 1
  }

  public static getPreviousPages(
    currentPage: number
  ): number[] {
    let previousPages: number[] = [];
    for (let page = 0; page < currentPage - 1; page++) {
      previousPages.push(page + 1);
    }

    return previousPages;
  }

  public static getNextPages(
    currentPage: number,
    totalPagesNumber: number
  ): number[] {
    let nextPages: number[] = [];
    for (let page = currentPage + 1; page <= totalPagesNumber; page++) {
      nextPages.push(page);
    }

    return nextPages;
  }
}