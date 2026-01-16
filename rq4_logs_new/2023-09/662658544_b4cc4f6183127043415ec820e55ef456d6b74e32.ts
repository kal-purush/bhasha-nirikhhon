import "dotenv/config";
import APIUri from "../api_uri.json";

const APIConfig: Record<string, string> = {};

const [auth_api, forum_api] = [
  // Only works when:
  //    1. the file .env doesn't exist
  //    2. (if needed) run jest with `--no-cache`
  process.env["AUTH_API"] as string,
  process.env["FORUM_API"] as string,
];

for (const [key, value] of Object.entries(APIUri)) {
  if (/^auth\./g.test(key)) {
    APIConfig[key] = `${auth_api}${value}`;
  } else {
    APIConfig[key] = `${forum_api}${value}`;
  }
}

export default APIConfig as Record<keyof typeof APIUri, string>;