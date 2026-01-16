const { defineConfig } = require('cypress');

module.exports = defineConfig({
  e2e: {
    chromeWebSecurity: false,
    experimentalShadowDomSupport: true,
    setupNodeEvents(on, config) {
      // Node event listeners here
    },
  },
});