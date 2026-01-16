import { ApiSettings, getApiDomainHeaders } from "@/shared/api";
import { PaginationState } from "@tanstack/react-table";
import { PermissionGrantUserFilters } from "./types";

export function createUserApi(settings: ApiSettings) {
  async function searchPermissionGrants({
    pagination,
    filters,
  }: {
    pagination: PaginationState;
    filters: PermissionGrantUserFilters;
  }) {
    return settings.client.POST("/private/permission_grant/user/search/v1", {
      params: {
        header: getApiDomainHeaders(settings),
        query: {
          lazyRelation: false,
          showPermissionGrantUser2PermissionSchemaMode: "DETAILED",
          showPermissionGrantUser2PermissionMode: "DETAILED",
          showPermissionGrantUser2UserGroupMode: "DETAILED",
          showPermissionGrantUser2UserMode: "DETAILED",
          showPermissionGrantUserMode: "DETAILED",
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

  return { searchPermissionGrants };
}

export type UserApi = ReturnType<typeof createUserApi>;