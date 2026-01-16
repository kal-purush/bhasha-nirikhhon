const video = require('wdio-video-reporter');
exports.config = {
  runner: 'local',
  path: '/',
  specs: [
    './test/specs/philly-to-san-fran-via-denver.js'
  ],
  exclude: [
    // 'path/to/excluded/files'
  ],
  maxInstances: 10,
  capabilities: [{
    maxInstances: 5,
    browserName: 'firefox',
  }],
  logLevel: 'info',
  bail: 0,
  baseUrl: 'http://localhost',
  waitforTimeout: 10000,
  connectionRetryTimeout: 90000,
  connectionRetryCount: 3,
  services: ['webdriver'],
  framework: 'mocha',
  reporters: [
    [video, {
      saveAllVideos: true,
      videoSlowdownMultiplier: 3
    }],
    'spec',
  ],
  mochaOpts: {
    ui: 'bdd',
    timeout: 60000
  },
  webDriverType: "geckodriver"
}