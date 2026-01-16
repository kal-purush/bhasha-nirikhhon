/** @type {CodeceptJS.MainConfig} */
exports.config = {
  name: "automacao-codeceptjs-web",
  tests: "./*_test.js",
  output: "./output",
  helpers: {
    WebDriver: {
      url: "https://demo.automationtesting.in/Index.html",
      browser: "chrome",
      waitForTimeout: 5000,
      desiredCapabilities: {
        chromeOptions: {},
      },
    },
  },
  include: {
    I: "./steps_file.js",
  },
  bootstrap: null,
  teardown: null,
  mocha: {},
  plugins: {
    wdio: {
      enabled: true,
      services: ["selenium-standalone"],
    },
  },
};