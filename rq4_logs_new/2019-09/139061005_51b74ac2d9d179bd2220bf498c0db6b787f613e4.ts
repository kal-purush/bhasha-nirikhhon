import { WindowLocation } from "@reach/router"
import queryString from "query-string"
import { ShareType } from "stores/NotificationStore"

export function parseShareParams(
  location: WindowLocation | undefined
): {
  share: string
  type: ShareType | ""
} {
  const qs = queryString.parse(location ? location.search : "")
  return {
    share: typeof qs.share === "string" ? qs.share : "",
    type:
      typeof qs.type === "string" &&
      (qs.type === "naytto" || qs.type === "tyossaoppiminen")
        ? qs.type
        : ""
  }
}

export function stringifyShareParams({
  share,
  type
}: {
  share: string
  type: ShareType
}): string {
  return queryString.stringify({ share, type })
}
queryString.stringify