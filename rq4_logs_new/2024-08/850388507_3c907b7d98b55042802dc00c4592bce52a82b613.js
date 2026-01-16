const { defineConfig } = require("cypress");

module.exports = defineConfig({
  e2e: {
    //baseUrl:'http://localhost:63352',
    excludeSpecPattern:['**/1-getting-started','**/2-advanced-examples'],
    specPattern:'cypress/e2e/**/*.{js,jsx,ts,tsx}',
    watchForFileChanges:false,
    setupNodeEvents(on, config) {
      // implement node event listeners here
    },
  },
});