import { notification } from "antd";

interface HandleApiCallParams<T, R> {
  call: () => ReturnType<typeof fetch>;
  handleResult: (json: T) => R;
  errContext: string;
  handleError?: (error: Error) => boolean | void | Promise<boolean | void>;
}

/**
 * Deals with API errors in a consistent manner and will surface a UI notification unless supressed by the `handleError` callback
 * @param call callback which returns a (`fetch`) `Response` object
 * @param handleResult callback which parses the API response. Whatever is returned here is passed through as the return value of `handleApiCall`
 * @param errContext Title of the generated error notification
 * @param handleError Optional callback which is passed caught errors, if return value is `false`, `handleApiCall` will not surface a notification
 */
export async function handleApiCall<T, R>({
  call,
  handleResult,
  errContext,
  handleError,
}: HandleApiCallParams<T, R>) {
  try {
    const response = await call();
    if (response.ok) {
      const json: T = await response.json();
      return await handleResult(json);
    } else {
      let errTxt;
      try {
        const { message } = await response.json();
        errTxt = `${response.status} ${response.statusText}\n"${message}"`;
      } catch {
        errTxt = `${response.status} ${response.statusText}`;
      }
      throw new Error(errTxt);
    }
  } catch (err) {
    const error = err instanceof Error ? err : new Error(JSON.stringify(err));
    const shouldPostNotif = handleError && ((await handleError(error)) ?? true);
    if (shouldPostNotif) {
      notification.open({
        message: errContext,
        description: error.message,
        type: "error",
      });
    }
  }
}