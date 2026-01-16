import { useMediaQuery } from "./use-media-query.js"

/**
 * Determine if current device is in portrait orientation.
 *
 * @returns true if portrait, false if landscape
 *
 * @example
 * Check if the device is in portrait mode.
 * ```tsx
 * import { useIsPortait } from "@trashpanda001/helpers/react"
 *
 * function Component() {
 *   const isPortrait = useIsPortrait()
 *   return <div>{isPortrait ? "portrait" : "landscape"}</div>
 * }
 * ```
 */
export function useIsPortrait() {
  return useMediaQuery("(orientation:portrait)")
}