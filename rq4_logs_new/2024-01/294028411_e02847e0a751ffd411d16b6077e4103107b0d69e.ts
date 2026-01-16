import { ENVIRONMENT } from '@app/environment';
import { generateTraceParent } from '@app/functions/generate-request-id';

const tabId = crypto.randomUUID();

enum HeaderKeys {
  TRACEPARENT = 'traceparent',
  VERSION = 'x-client-version',
  TAB_ID = 'x-tab-id',
}

enum QueryKeys {
  TRACEPARENT = 'traceparent',
  VERSION = 'version',
  TAB_ID = 'tabId',
}

export const getHeaders = () => ({
  [HeaderKeys.TRACEPARENT]: generateTraceParent(),
  [HeaderKeys.VERSION]: ENVIRONMENT.version,
  [HeaderKeys.TAB_ID]: tabId,
});

export const setHeaders = (headers: Headers): Headers => {
  headers.set(HeaderKeys.TRACEPARENT, generateTraceParent());
  headers.set(HeaderKeys.VERSION, ENVIRONMENT.version);
  headers.set(HeaderKeys.TAB_ID, tabId);

  return headers;
};

export const getQueryParams = () => {
  const { version } = ENVIRONMENT;
  const traceParent = generateTraceParent();

  return `${QueryKeys.VERSION}=${version}&${QueryKeys.TAB_ID}=${tabId}&${QueryKeys.TRACEPARENT}=${traceParent}`;
};