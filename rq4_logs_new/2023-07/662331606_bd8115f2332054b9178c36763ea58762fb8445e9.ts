import {PaginatorData} from "../models/paginator-data";
import {PaginatorPageInfoHandler} from "./paginator-page-info-builder";
import {PaginatorShownItemsInfoHandler} from "./paginator-shown-items-info-builder";
import {PaginatorPageHelper} from "../paginator-page-helper";

export class PaginatorDataBuilder {
  public static build(
    itemsPerPage: number,
    skippedItemsNumber: number,
    shownItemsNumber: number,
    totalItemsNumber: number,
  ): PaginatorData {
    let result: PaginatorData = new PaginatorData();

    result.currentPage = PaginatorPageHelper.getCurrentPage(skippedItemsNumber, itemsPerPage);
    result.totalPages = PaginatorPageHelper.getTotalPagesNumber(totalItemsNumber, itemsPerPage);

    result.pageInfos = PaginatorPageInfoHandler.buildPageInfos(
      result.currentPage,
      result.totalPages
    );

    result.shownItemsInfo = PaginatorShownItemsInfoHandler.buildShownItemsInfo(
      skippedItemsNumber,
      shownItemsNumber,
      totalItemsNumber
    );

    return result;
  }
}