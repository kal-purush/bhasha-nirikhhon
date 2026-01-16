import { ApiContext } from "@/shared/api";
import { PaginationState } from "@tanstack/react-table";
import { useCallback, useContext } from "react";
import { TwinFlow } from "../types";

// TODO: Apply caching-strategy
export const useTwinFlowSearchV1 = () => {
  const api = useContext(ApiContext);

  const searchTwinFlows = useCallback(
    async ({
      twinClassId,
      pagination = { pageIndex: 0, pageSize: 10 },
    }: {
      twinClassId: string;
      pagination?: PaginationState;
    }): Promise<{ data: TwinFlow[]; pageCount: number }> => {
      try {
        const { data, error } = await api.twinflow.search({
          twinClassId,
          pagination,
        });

        if (error) {
          console.error("Failed to fetch twin flows due to API error:", error);
          throw new Error("Failed to fetch twin flows due to API error");
        }

        const twinFlows = data.twinflowList ?? [];

        const totalItems = data.pagination?.total ?? 0;
        const pageCount = Math.ceil(totalItems / pagination.pageSize);

        console.log("Fetched twin flows:", twinFlows);
        return { data: twinFlows, pageCount };
      } catch (error) {
        console.error("Failed to fetch twin flows:", error);
        throw new Error("An error occurred while fetching twin flows");
      }
    },
    [api]
  );

  return { searchTwinFlows };
};