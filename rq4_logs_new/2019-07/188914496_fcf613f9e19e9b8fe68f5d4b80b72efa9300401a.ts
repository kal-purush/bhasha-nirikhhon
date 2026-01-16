import { Light, LightsQuery } from "generated/graphql";
import { DataProxy } from "apollo-cache";
import { LIGHTS } from "./graphqlConstants";

export const removeLightFromCache = (proxy: DataProxy, lightToRemove: Light): void => {
  try {
    const result = proxy.readQuery<LightsQuery>({
      query: LIGHTS,
    });

    if (!result || !result.lights) return;

    const { lights } = result;
    proxy.writeQuery({
      query: LIGHTS,
      data: {
        lights: lights.filter((light): boolean => light.id !== lightToRemove.id),
      },
    });
  } catch (error) {
    // If there is an invariant error, this shouldnt be an issue so ignore it
    if (error.name === "Invariant Violation") return;
    throw error;
  }
};