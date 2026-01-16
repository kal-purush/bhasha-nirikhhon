import type { AspidaClient, BasicHeaders } from 'aspida';
import type { Methods as Methods_1ybobht } from './auth/signUp';
import type { Methods as Methods_1hyhucz } from './auth/validateSignUp';

const api = <T>({ baseURL, fetch }: AspidaClient<T>) => {
  const prefix = (baseURL === undefined ? '' : baseURL).replace(/\/$/, '');
  const PATH0 = '/auth/signUp';
  const PATH1 = '/auth/validateSignUp';
  const POST = 'POST';

  return {
    auth: {
      signUp: {
        /**
         * 会員登録実行
         * @returns 会員登録成功
         */
        post: (option: { body: Methods_1ybobht['post']['reqBody']; config?: T | undefined }) =>
          fetch<
            Methods_1ybobht['post']['resBody'],
            BasicHeaders,
            Methods_1ybobht['post']['status']
          >(prefix, PATH0, POST, option).json(),
        /**
         * 会員登録実行
         * @returns 会員登録成功
         */
        $post: (option: { body: Methods_1ybobht['post']['reqBody']; config?: T | undefined }) =>
          fetch<
            Methods_1ybobht['post']['resBody'],
            BasicHeaders,
            Methods_1ybobht['post']['status']
          >(prefix, PATH0, POST, option)
            .json()
            .then((r) => r.body),
        $path: () => `${prefix}${PATH0}`,
      },
      validateSignUp: {
        /**
         * 会員登録のバリデーションチェック
         * @returns バリデーションチェック成功
         */
        post: (option: { body: Methods_1hyhucz['post']['reqBody']; config?: T | undefined }) =>
          fetch<
            Methods_1hyhucz['post']['resBody'],
            BasicHeaders,
            Methods_1hyhucz['post']['status']
          >(prefix, PATH1, POST, option).json(),
        /**
         * 会員登録のバリデーションチェック
         * @returns バリデーションチェック成功
         */
        $post: (option: { body: Methods_1hyhucz['post']['reqBody']; config?: T | undefined }) =>
          fetch<
            Methods_1hyhucz['post']['resBody'],
            BasicHeaders,
            Methods_1hyhucz['post']['status']
          >(prefix, PATH1, POST, option)
            .json()
            .then((r) => r.body),
        $path: () => `${prefix}${PATH1}`,
      },
    },
  };
};

export type ApiInstance = ReturnType<typeof api>;
export default api;