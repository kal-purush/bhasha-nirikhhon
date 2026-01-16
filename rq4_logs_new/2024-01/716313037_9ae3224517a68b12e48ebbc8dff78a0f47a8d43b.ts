// utils/meta.ts
import { invariant } from "@epic-web/invariant";

import type { SerializeFrom } from "@remix-run/node";
import type { MetaMatches } from "@remix-run/react/dist/routeModules";

import type { loader as rootLoader } from "#app/root";

type RouteLoaders = {
  root: typeof rootLoader;
};

export function routeLoaderData<K extends keyof RouteLoaders>(
  matches: MetaMatches,
  matchId: K,
): SerializeFrom<RouteLoaders[K]> {
  const match = matches.find(({ id }) => id === matchId);
  invariant(match, "Router loader data not found");

  return match.data as SerializeFrom<RouteLoaders[K]>;
}