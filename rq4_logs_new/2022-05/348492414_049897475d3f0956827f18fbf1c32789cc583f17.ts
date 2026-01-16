/* eslint-disable @typescript-eslint/ban-ts-comment */
/* eslint-disable import/no-extraneous-dependencies */

// From: https://blog.dai.codes/cypress-loading-state-tests/
// Allows to test that the loading icon is displayed until the data is fully loaded
import {
  HttpResponseInterceptor,
  RouteMatcher,
  StaticResponse,
} from 'cypress/types/net-stubbing';

export function interceptIndefinitely(
  requestMatcher: RouteMatcher,
  response?: StaticResponse | HttpResponseInterceptor
): { sendResponse: () => void } {
  let sendResponse;
  const trigger = new Promise((resolve) => {
    sendResponse = resolve;
  });
  cy.intercept(requestMatcher, (request) =>
    trigger.then(() => {
      request.reply(response);
    })
  );
  // @ts-ignore
  return { sendResponse };
}