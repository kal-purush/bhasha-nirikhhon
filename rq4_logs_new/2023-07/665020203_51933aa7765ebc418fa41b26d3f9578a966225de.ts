export const BASE_URL = "https://api.github.com";

export const REPO_INFO = "facebook/react";

export const API = {
  ISSUES: `search/issues?q=is:issue+is:open+repo:${REPO_INFO}`,
  DETAIL: `/repos/${REPO_INFO}/issues`,
};