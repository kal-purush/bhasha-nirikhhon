// const withLoading = async <T>(set: any, fn: () => Promise<T>): Promise<T> => {
import { toast } from 'sonner';

export const withLoading = async <T, S extends { isLoading: boolean }>(
  set: (fn: (state: S) => void) => void,
  get: () => S,
  fn: () => Promise<T>,
): Promise<T> => {
  const state = get();

  if (state.isLoading) {
    toast.warning('기다려주세요', { description: '요청을 처리 중입니다.' });
    return Promise.resolve(false as T);
  }

  set(state => {
    state.isLoading = true;
  });

  try {
    return await fn();
  } finally {
    set(state => {
      state.isLoading = false;
    });
  }
};