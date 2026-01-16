import type { AxiosError } from 'axios';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const withConnRefusedCatch =
  <P extends any[], R = void>(
    serviceFn: (...args: P) => Promise<R>,
    initialResult?: R
  ): ((...args: P) => Promise<R>) =>
  async (...args: P) => {
    try {
      return await serviceFn(...args);
    } catch (e) {
      const { code, response } = e as AxiosError;

      if (
        /^(development|test)$/.test(process.env.NODE_ENV) &&
        (code === 'ECONNREFUSED' || response?.status === 404)
      ) {
        return initialResult as R;
      }

      throw e;
    }
  };