import { ApiSettings, getApiDomainHeaders } from "@/shared/api";
import { PaginationState } from "@tanstack/table-core";
import { operations } from "@/shared/api/generated/schema";
import { DatalistApiFilters } from "@/entities/datalist";

export function createDatalistApi(settings: ApiSettings) {
  function searchDatalist({
    pagination,
    filters,
  }: {
    pagination: PaginationState;
    filters: DatalistApiFilters;
  }) {
    return settings.client.POST("/private/data_list/search/v1", {
      params: {
        header: getApiDomainHeaders(settings),
        query: {
          showDataListMode: "DETAILED",
          offset: pagination.pageIndex * pagination.pageSize,
          limit: pagination.pageSize,
          sortAsc: false,
        },
      },
      body: {
        ...filters,
      },
    });
  }

  function getById({
    id,
    query = {},
  }: {
    id: string;
    query: operations["dataListPublicViewV1"]["parameters"]["query"];
  }) {
    return settings.client.GET(`/public/data_list/{dataListId}/v1`, {
      params: {
        header: { ...getApiDomainHeaders(settings), Locale: "en" },
        path: { dataListId: id },
        query: query,
      },
    });
  }

  return { searchDatalist, getById };
}

export type DatalistApi = ReturnType<typeof createDatalistApi>;