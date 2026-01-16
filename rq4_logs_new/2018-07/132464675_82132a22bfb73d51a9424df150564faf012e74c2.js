import {environment} from "../config/projectProperties.js";

/**
 * Listener for environment URL update
 */
chrome.runtime.onMessage.addListener((request) => {
    if(request.URLChange) {
        environment.defaultEnvironmentURL = request.URLChange;
    }
});

/**
 * Check if there is default URL set in storage and if it is - use it
 */
export let checkDefaultURL = () => {
  chrome.storage.local.get(['defaultURL'], function (result) {
      if(result.defaultURL) {
          environment.defaultEnvironmentURL = result;
      }
  })
};