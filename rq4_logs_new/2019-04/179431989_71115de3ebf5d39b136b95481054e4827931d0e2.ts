import fetch, { FetchError, RequestInit } from 'node-fetch';

const constructEndpointUri = (rootEndpoint: string, functionName: string) =>
  rootEndpoint + functionName;

export const fetchFn = (
  functionName: string,
  init?: RequestInit | undefined,
  rootEndpoint: string = process.env.CLOUD_FUNCTION_ROOT_ENDPOINT!
) => {
  return fetch(constructEndpointUri(rootEndpoint, functionName), init);
};

/**
 * Call function using Firebase HTTP call conventions
 * @param functionName Name of the function
 * @param authToken Authentication token
 * @param rootEndpoint Functions root endpoint
 */
export const callFn = <T, P = unknown>(
  functionName: string,
  authToken?: string,
  rootEndpoint: string = process.env.CLOUD_FUNCTION_ROOT_ENDPOINT!
) => (data: P) =>
  fetch(constructEndpointUri(rootEndpoint, functionName), {
    method: 'post',
    body: JSON.stringify({ data }),
    headers: {
      'Content-Type': 'application/json',
      'cache-control': 'no-cache',
      ...(authToken && { Authorization: 'Bearer ' + authToken }),
    },
    redirect: 'error',
  }).then(async res => {
    if (res.ok) {
      return res.json() as Promise<T>;
    }

    throw new FetchError(
      res.statusText,
      `HTTP STATUS: ${res.status}`,
      res.type
    );
  });