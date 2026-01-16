import type { ILogLevel } from "#lib/logs";
import { NEXT_PUBLIC_VERCEL_URL } from "./vercel";

export const NODE_ENV = process.env.NODE_ENV;

export const SITE_ORIGIN =
  NEXT_PUBLIC_VERCEL_URL ?? process.env.NEXT_PUBLIC_ORIGIN!;

if (!SITE_ORIGIN) {
  throw new Error(`"SITE_ORIGIN" is not set.`);
}

export const SITE_TITLE = process.env.NEXT_PUBLIC_SITE_TITLE!;

if (!SITE_TITLE) {
  throw new Error(`"SITE_TITLE" is not set.`);
}

export const REPOSITORY_URL = process.env.NEXT_PUBLIC_REPOSITORY_URL!;

if (!REPOSITORY_URL) {
  throw new Error(`"REPOSITORY_URL" is not set.`);
}

/**
 * @TODO somehow validate this value without cyclic dependencies.
 */
export const DEFAULT_LOG_LEVEL = process.env
  .NEXT_PUBLIC_DEFAULT_LOG_LEVEL as ILogLevel;

export const IS_BROWSER = typeof window !== "undefined";
export const IS_DEVELOPMENT = NODE_ENV === "development";