/**
 * @file This file contains functionality related to errors tha can be thrown during sending the request and receiving response to HTTP backend.
 */

/**
 * This error is thrown when the backend returns something else than `200` or `204` as status code.
 * Notice that only type information about this is exported, not the class itself.
 */
export class Non2xxStatusCodeError extends Error {
  /**
   * Creates new instance of this error with given parameters.
   * @param statusCode The status code returned by backend.
   */
  public constructor(public readonly statusCode: number) {
    super(`Status code ${statusCode} was returned.`);
  }
}

/**
 * Helper function to test whether some error is {@link Non2xxStatusCodeError}.
 * @param error The {@link Error} to test.
 * @returns `true` if given `error` is {@link Non2xxStatusCodeError}, `false` otherwise.
 */
export const isNon2xxStatusCodeError = (
  error: unknown,
): error is Non2xxStatusCodeError => error instanceof Non2xxStatusCodeError;