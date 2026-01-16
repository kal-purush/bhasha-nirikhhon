import { ApiContext } from "@/shared/api";
import { isUndefined } from "@/shared/libs";
import { useCallback, useContext } from "react";
import { TwinFlowTransition } from "../types";

// TODO: Apply caching-strategy after discussing with team
export const useFetchTwinFlowTransitionById = () => {
  const api = useContext(ApiContext);

  const fetchTwinFlowTransitionById = useCallback(
    async (id: string): Promise<TwinFlowTransition> => {
      const { data, error } = await api.twinFlowTransition.fetchById(id);

      if (error) {
        throw new Error(
          "Failed to fetch twin-flow-transition due to API error",
          error
        );
      }

      if (isUndefined(data.transition)) {
        throw new Error(`Twin-flow-transition with ID ${id} not found`);
      }

      return data.transition;
    },
    [api]
  );

  return { fetchTwinFlowTransitionById };
};