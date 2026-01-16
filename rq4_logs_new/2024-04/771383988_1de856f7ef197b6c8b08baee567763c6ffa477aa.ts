import {
  type SdkBaseOptions as Options,
  endpoint,
  getSdk,
  getSdkWithHooks,
  initGraphQLClient,
} from '@yuntijs/yunti-bff-sdk';
import qs from 'query-string';
import { useMemo } from 'react';

export { initSdk, sdk } from '@yuntijs/yunti-bff-sdk';

export interface SdkBaseOptions extends Options {
  /** 分支版本等 */
  tree?: string;
}

/** 初始化 sdk */
export const initSdkBase = (options: SdkBaseOptions = {}) => {
  const { url: _url, withHooks, requestConfig, tree } = options;

  let url = _url || endpoint;
  const [host, search] = url.split('?');
  const query = qs.parse(search);
  if (tree) {
    query.tree = tree;
  }
  if (Object.keys(query).length > 0) {
    url = `${host}?${qs.stringify(query)}`;
  }

  const newClient = initGraphQLClient(url, requestConfig);
  // 注意：切记 SWR 缓存的唯一依据是 key，一切变量都需要放到 key 中，否则即使是不同的 client，key 相同的话，
  // 也会以第一次调用产生的实例进行数据请求
  const newSdk = withHooks ? getSdkWithHooks(newClient, tree) : getSdk(newClient);
  return newSdk;
};

export type SdkOptions = Pick<SdkBaseOptions, 'url' | 'requestConfig' | 'tree'>;

/** 初始化 sdk 实例 (包含 hooks) */
export const initSdkWithHooks = (options: SdkOptions = {}) => {
  const { url, requestConfig, tree } = options;
  const newSdk = initSdkBase({ withHooks: true, url, requestConfig, tree });
  return newSdk as ReturnType<typeof getSdkWithHooks>;
};

/**
 * hook 的方式获取 sdk 实例
 *
 * @param {SdkOptions} options 配置项
 */
export const useSdk = (options: SdkOptions = {}) => {
  const { url, requestConfig, tree } = options;
  return useMemo(() => initSdkWithHooks({ url, requestConfig, tree }), [url, requestConfig, tree]);
};