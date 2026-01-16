/**
 * @file This file contains code used by all TyRAS client-specific libraries.
 */

/**
 * This callback type provides a way to customize additional configuration when TyRAS sends any HTTP request.
 */
export type HTTPRequestConfigProcessor<TConfig extends object> = (
  requestConfig: TConfig,
) => void | Promise<void>;

/**
 * This function should be used before passing given pathname -string to {@link URL} constructor, to avoid `?` or `#` or other characters possibly present in the pathname to be interpreted as query or fragment.
 * @param pathname The pathname that may contain `?` or `#` or other interpretable characters.
 * @returns String where all `?` or `#` are replaced with [percentage-encoding](https://www.w3schools.com/tags/ref_urlencode.ASP).
 */
export const ensurePathname = (pathname: string) => {
  const url = new URL("/dummy", "http://dummy.net");
  // Setter will do necessary escaping
  url.pathname = pathname;
  // After setting, return the value via getter, which now will be safe to use
  return url.pathname;
};

/**
 * Creates a new {@link URLSearchParams} from given query parameters.
 * If some parameter is an array, then its values are unwrapped so that result {@link URLSearchParams} will use correct query array parameter encoding.
 * @param query The query parameters as named objects.
 * @returns The {@link URLSearchParams} with arrayed query parameters unwrapped.
 */
export const getURLSearchParams = (query: Record<string, unknown>) =>
  new URLSearchParams(
    Object.entries(query)
      .filter(([, value]) => value !== undefined)
      .flatMap<[string, string]>(([qKey, qValue]) =>
        Array.isArray(qValue)
          ? qValue.map<[string, string]>((value) => [qKey, `${value}`])
          : [[qKey, `${qValue}`]],
      ),
  );

/**
 * Converts given header values into suitable header values for HTTP request (string, number, or array of strings).
 * @param headers The optional headers to be converted.
 * @returns Headers for HTTP request.
 * @see getOutgoingHeader
 */
export function getOutgoingHeaders(
  headers: Record<string, unknown> | undefined,
): Record<string, OutgoingHttpHeader> | undefined;

/**
 * Converts given header values into suitable header values for HTTP request (string, or array of strings, but no number).
 * @param headers The optional headers to be converted.
 * @param noNumbers Set to `true` in order to not return numbers: only strings or arrays of strings.
 * @returns Headers for HTTP request.
 * @see getOutgoingHeader
 */
export function getOutgoingHeaders(
  headers: Record<string, unknown> | undefined,
  noNumbers: true,
): Record<string, OutgoingHttpHeaderNoNumber> | undefined;

/**
 * Converts given header values into suitable header values for HTTP request (string, or array of strings, but no number).
 * @param headers The optional headers to be converted.
 * @param noNumbers If set to `true`, only strings or array of strings will be returned.
 * @returns Headers for HTTP request.
 * @see getOutgoingHeader
 */
export function getOutgoingHeaders(
  headers: Record<string, unknown> | undefined,
  noNumbers?: boolean,
): Record<string, OutgoingHttpHeader> | undefined;

/**
 * Converts given header values into suitable header values for HTTP request (string, or array of strings, but no number).
 * @param headers The optional headers to be converted.
 * @param noNumbers If set to `true`, only strings or array of strings will be returned.
 * @returns Headers for HTTP request.
 * @see getOutgoingHeader
 */
export function getOutgoingHeaders(
  headers: Record<string, unknown> | undefined,
  noNumbers?: boolean,
): Record<string, OutgoingHttpHeader> | undefined {
  return headers === undefined
    ? undefined
    : Object.fromEntries(
        Object.entries(headers)
          .filter(([, header]) => header !== undefined)
          .map(
            ([headerName, header]) =>
              [headerName, getOutgoingHeader(header, noNumbers)] as const,
          ),
      );
}

/**
 * Converts the given header value into string, number, or array of strings, to be used as HTTP request headers.
 * @param header The header as some value.
 * @returns The value to be used as HTTP request header value.
 */
export function getOutgoingHeader(header: unknown): OutgoingHttpHeader;

/**
 * Converts the given header value into string, or array of strings, to be used as HTTP request headers.
 * @param header The header as some value.
 * @param noNumbers Set to `true` in order to not to return numbers: only strings or arrays of strings.
 * @returns The value to be used as HTTP request header value.
 */
export function getOutgoingHeader(
  header: unknown,
  noNumbers: true,
): OutgoingHttpHeaderNoNumber;

/**
 * Converts the given header value into string or array of strings, or number if so specified.
 * @param header The header as some value.
 * @param noNumbers If set to `true`, only strings or array of strings will be returned.
 * @returns The value to be used as HTTP request header value.
 */
export function getOutgoingHeader(
  header: unknown,
  noNumbers?: boolean,
): OutgoingHttpHeader;

/**
 * Converts the given header value into string or array of strings, or number if so specified.
 * @param header The header as some value.
 * @param noNumbers If set to `true`, only strings or array of strings will be returned.
 * @returns The value to be used as HTTP request header value.
 */
export function getOutgoingHeader(
  header: unknown,
  noNumbers?: boolean,
): OutgoingHttpHeader {
  return typeof header === "string" ||
    (typeof header === "number" && noNumbers !== true)
    ? header
    : Array.isArray(header)
    ? header.filter((v) => v !== undefined).map((v) => `${v}`)
    : `${header}`;
}

/**
 * This is the type for values of HTTP request headers, sans `number` type.
 */
export type OutgoingHttpHeaderNoNumber = string | Array<string>;

/**
 * This is the type for values of HTTP request headers, including `number` type.
 */
export type OutgoingHttpHeader = OutgoingHttpHeaderNoNumber | number;