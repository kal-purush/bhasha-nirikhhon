import { ApiContext } from "@/shared/api";
import { isUndefined } from "@/shared/libs";
import { useCallback, useContext } from "react";
import { TwinFlow } from "../types";

// TODO: Apply caching-strategy
export const useTwinFlowFetchByIdV1 = () => {
  const api = useContext(ApiContext);

  const fetchTwinFlowById = useCallback(
    async (id: string): Promise<TwinFlow> => {
      const { data, error } = await api.twinflow.getById({
        twinFlowId: id,
      });

      if (error) {
        console.error("API error while fetching twin flows:", error);
        throw new Error("Failed to fetch twin flows due to API error");
      }

      if (isUndefined(data.twinflow)) {
        throw new Error("Invalid response data while fetching twin flows");
      }

      return data.twinflow;
    },
    [api]
  );

  return { fetchTwinFlowById };
};