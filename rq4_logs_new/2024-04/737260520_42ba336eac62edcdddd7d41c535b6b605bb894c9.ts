import { Endpoint } from "../store";

export function getCandidateTitleEndpoints(
  endpoints: Endpoint[],
  current: Endpoint,
) {
  if (current.genTitle) {
    return current;
  }

  const candidates = endpoints.filter((v) => v.genTitle && v.type !== "system");
  if (candidates.length === 0) {
    return null;
  }

  //从candidates随机取一个
  const c = candidates[Math.floor(Math.random() * candidates.length)];
  if (c) {
    return c;
  }

  return endpoints.find((v) => v.type === "system");
}