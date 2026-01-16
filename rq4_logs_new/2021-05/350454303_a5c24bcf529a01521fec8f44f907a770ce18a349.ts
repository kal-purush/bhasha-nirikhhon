import setCookie from 'set-cookie-parser';
import { Request as ExpressRequest, Response as ExpressResponse } from 'express';
import { Response as FetchResponse } from 'node-fetch';
import { serialize } from 'cookie';

export const getRequestCookies = (expressRequest: ExpressRequest) => {
  const { cookie } = expressRequest.headers;

  return { cookie };
};

const getSetCookie = (response: FetchResponse) =>
  setCookie.parse(response.headers.raw()('set-cookie'), {
    decodeValues: true,
  });

export const getResponseCookies = (fetchResponse: FetchResponse) => {
  const cookies = getSetCookie(fetchResponse);

  const cookie = cookies
    .map(function (cookie) {
      return serialize(cookie.name, cookie.value);
    })
    .join('; ');

  return { cookie };
};

export const setCookies = (fetchResponse: FetchResponse, expressResponse: ExpressResponse) => {
  const cookies = getSetCookie(fetchResponse);

  cookies.forEach(({ name, value }) => {
    expressResponse.cookie(name, value, { secure: true, httpOnly: true });
  });
};