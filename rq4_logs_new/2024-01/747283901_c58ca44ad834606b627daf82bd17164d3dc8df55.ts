/* eslint-disable @typescript-eslint/naming-convention */
// The naming convention for the configuration object doesn't match our project styling
// but the transpiled JS version of this is used as a config file for BrowserStack
// so we're beholden to their formatting in this case

import {
  addBrowsers,
  browserCapabilities,
  Configuration
} from "./config";

// See https://www.browserstack.com/automate/capabilities
const BSLOCAL_CAPABILITIES = {
  'browserstack.user': process.env.BROWSERSTACK_USERNAME,
  'browserstack.key': process.env.BROWSERSTACK_ACCESS_KEY,
  'browserstack.local': true,
  'name': 'Bstack-[Nightwatch] Local Test'
};

const localDirectory = __dirname;

const nightwatchConfig: Configuration = {
  src_folders: [localDirectory],
  page_objects_path: [localDirectory + "/page_objects"],
  custom_assertions_path: [],
  disable_typescript: true,

  selenium : {
    "start_process" : false,
    "host" : "hub-cloud.browserstack.com",
    "port" : 443,

    //"proxy": "http://PROXY_USERNAME:PROXY_PASSWORD@proxy-host:proxy-port"  // If you are behind a proxy
  },

  globals_path: '',

  test_settings: {
    default: {
      desiredCapabilities: {
        ...BSLOCAL_CAPABILITIES,
        ...browserCapabilities('chrome', 'latest', 'Windows', '10'),
      }
    }
  }
};

// Matrix over OSes/browsers that we want to use
const environments = nightwatchConfig.test_settings;

// Windows
const WINDOWS_VERSIONS = ["10", "8.1", "7"];
const WINDOWS_BROWSERS = ["Chrome", "MicrosoftEdge",  "Firefox", "IE"];
const winKeyMaker = function(version: string, browser: string): string {
  const browserName = (browser === 'MicrosoftEdge' ? 'Edge' : browser);
  return `${browserName}_Win${version}`.replace(" ", "");
};
addBrowsers(environments, BSLOCAL_CAPABILITIES, 'Windows', WINDOWS_VERSIONS, WINDOWS_BROWSERS, winKeyMaker);

// OS X
const OSX_VERSIONS = ["Big Sur", "Catalina", "Mojave"];
const OSX_BROWSERS = ["Chrome", "MicrosoftEdge", "Firefox", "Safari"];
const osxKeyMaker = function(version: string, browser: string): string {
  const browserName = (browser === 'MicrosoftEdge' ? 'Edge' : browser);
  return `${browserName}_${version}`.replace(" ", "");
};
addBrowsers(environments, BSLOCAL_CAPABILITIES, 'OS X', OSX_VERSIONS, OSX_BROWSERS, osxKeyMaker);

// For convenience, add the latest versions of browsers on Windows
// (except Safari, for which we use OS X)
// to an environment named `<lowercasebrowsername>`
const simpleKeyMaker = function(_version: string, browser: string): string {
  if (browser === 'MicrosoftEdge') {
    return 'edge';
  }
  return browser.toLowerCase();
};
addBrowsers(environments, BSLOCAL_CAPABILITIES, 'Windows', ['10'], ['Chrome', 'Firefox', 'MicrosoftEdge'], simpleKeyMaker);
addBrowsers(environments, BSLOCAL_CAPABILITIES, 'OS X', ['Ventura'], ['Safari'], simpleKeyMaker);

// Code to copy seleniumhost/port into test settings
for (const i in nightwatchConfig.test_settings) {
  const config = nightwatchConfig.test_settings[i];
  if (config === undefined || nightwatchConfig.selenium === undefined) {
    continue;
  }
  config['selenium_host'] = nightwatchConfig.selenium.host;
  config['selenium_port'] = nightwatchConfig.selenium.port;
}
module.exports = nightwatchConfig;