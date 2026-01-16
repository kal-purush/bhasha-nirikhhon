import {
  createProxyMiddleware,
  fixRequestBody,
  responseInterceptor,
} from 'http-proxy-middleware';
import {
  API_YANDEX_DOMAIN,
  APP_CURRENT_DOMAIN,
  PROXY_PATH_REWRITE_OPTIONS,
} from '../utils/const';
import { userInterceptorHandler } from '../utils/user';
import isJsonString from '../utils/json-check';

const options = {
  target: API_YANDEX_DOMAIN,
  pathRewrite: PROXY_PATH_REWRITE_OPTIONS,
  cookieDomainRewrite: APP_CURRENT_DOMAIN,
  changeOrigin: true,
  secure: false,
  timeout: 10000,
  onProxyReq: fixRequestBody,
  selfHandleResponse: true,
  onProxyRes: responseInterceptor(
    async (responseBuffer, _proxyRes, _req, response) => {
      const bufferData = responseBuffer.toString('utf8');

      let data;
      if (
        response
          .getHeader('content-type')
          ?.toString()
          .includes('application/json') &&
        isJsonString(bufferData)
      ) {
        data = JSON.parse(bufferData);
        // entity is user
        if (data.id && data.email) {
          const user = await userInterceptorHandler(data);
          data = user ? JSON.stringify(user) : data;
        } else {
          data = responseBuffer;
        }
      }
      response.setHeader('HPM-Header', 'Intercepted by HPM');
      response.setHeader(
        'Cache-Control',
        'no-cache, no-store, must-revalidate'
      );
      response.setHeader('Pragma', 'no-cache');
      response.setHeader('Expires', '0');
      return data || responseBuffer;
    }
  ),
};

export default createProxyMiddleware(options);