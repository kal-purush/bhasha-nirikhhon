import { PaginationParams } from '@github-commit-history-viewer/shared/types';

export const updateUrlQueryParams = (urlStr: string, queryParams: PaginationParams) => {
  const url = new URL(urlStr);
  for (const [key, value] of Object.entries(queryParams)) {
    url.searchParams.set(key, String(value));
  }

  return url.href;
};

export const validateGitHubURL = (url: string) => {
  const githubURLPattern = /^https:\/\/github\.com\/([A-Za-z0-9-_.]+)\/([A-Za-z0-9-_.]+)$/;

  if (githubURLPattern.test(url)) {
    const match = url.match(githubURLPattern);
    if (!match || !match.length) {
      return { isValid: false };
    }
    const username = match[1];
    const projectName = match[2];
    return {
      isValid: true,
      username,
      projectName,
      paramRoute: `${username}/${projectName}`,
    };
  } else {
    return { isValid: false };
  }
};