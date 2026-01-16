const { defineConfig } = require('cypress')

module.exports = defineConfig({
  e2e: {
    baseUrl: 'http://localhost:8100', // Your Ionic dev server
    supportFile: false,
  },
})