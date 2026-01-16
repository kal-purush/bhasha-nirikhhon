import { siteConfig } from "@/config/site";

export function sanitizeRedirect(path: string) {
  return /^\/[a-zA-Z0-9/_-]*$/.test(path) ? path : siteConfig.routes.home;
}