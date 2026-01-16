import { ApiContext } from "@/shared/api";
import { PaginationState } from "@tanstack/react-table";
import { useCallback, useContext } from "react";
import { hydratePermissionFromMap } from "../../libs";
import { Permission_DETAILED, PermissionApiFilters } from "../types";

// TODO: Apply caching-strategy
export const usePermissionSearchV1 = () => {
  const api = useContext(ApiContext);

  const searchPermissions = useCallback(
    async ({
      pagination = { pageIndex: 0, pageSize: 10 },
      filters = {},
    }: {
      search?: string;
      pagination?: PaginationState;
      filters?: PermissionApiFilters;
    }): Promise<{ data: Permission_DETAILED[]; pageCount: number }> => {
      try {
        const { data, error } = await api.permission.search({
          pagination,
          filters,
        });

        if (error) {
          console.error("Failed to fetch permissions due to API error:", error);
          throw new Error("Failed to fetch permissions due to API error");
        }

        const permissions =
          data.permissions?.map((dto) =>
            hydratePermissionFromMap(dto, data.relatedObjects)
          ) ?? [];

        const totalItems = data.pagination?.total ?? 0;
        const pageCount = Math.ceil(totalItems / pagination.pageSize);

        console.log("Fetched permissions:", permissions);
        return { data: permissions, pageCount };
      } catch (error) {
        console.error("Failed to fetch twin classes:", error);
        throw new Error("An error occurred while fetching twin classes");
      }
    },
    [api]
  );

  return { searchPermissions };
};