import type { RequestListener } from 'node:http';
import { Readable } from 'node:stream';
import type { ReadableStream as NodeReadableStream } from 'node:stream/web';

import { importEndpoints } from '../file-based-routing/index.js';
import { errorHandler, toPathRegExp } from '../index.js';

/**
 * File based routing for node:http. Creates a RequestListener that handles requests with
 * the matching FetchHandler.
 *
 * @param router the express router, for example `express()` or `express.Router()`.
 * @param routeDir the directory containing the routes.
 */
export async function createRequestListener(routeDir: string): Promise<RequestListener> {
  const endpoints = await importEndpoints(routeDir);

  const requestListener: RequestListener = (incomingMessage, serverResponse) => {
    if (incomingMessage.url === undefined) {
      throw new Error('url is undefined');
    }
    if (incomingMessage.headers.host === undefined) {
      throw new Error('url is undefined');
    }
    const url = new URL(incomingMessage.url, `http://${incomingMessage.headers.host}`);
    const requestBody = Readable.toWeb(incomingMessage);
    for (const { method, pathPattern, handler } of endpoints) {
      if (method !== incomingMessage.method) {
        continue;
      }
      const pathRegExp = toPathRegExp(pathPattern);
      if (!pathRegExp.test(url.pathname)) {
        continue;
      }
      const requestInit: RequestInit = {
        method,
        headers: new Headers(incomingMessage.headers as Record<string, string>),
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        body: requestBody,
        // https://github.com/nodejs/node/issues/46221#issuecomment-1426707013
        duplex: 'half',
      };
      const request = new Request(url, requestInit);
      const eh = errorHandler(handler);
      eh(request).then((response) => {
        serverResponse.statusCode = response.status;
        response.headers.forEach((value, key) => {
          serverResponse.setHeader(key, value);
        });
        if (response.body) {
          Readable.fromWeb(response.body as NodeReadableStream).pipe(serverResponse);
        } else {
          serverResponse.end();
        }
      });
      return;
    }

    // No matching route
    // TODO: 404
  };
  return requestListener;
}