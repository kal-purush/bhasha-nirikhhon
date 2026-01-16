/* eslint-env jest */
import { Browser } from "selenium-webdriver";
import yn from "yn";

import SingletonWebDriver from "./tests/helpers/webdriver/SingletonWebDriver";

jest.setTimeout(60 * 1000);

beforeAll(async () => {
  try {
    SingletonWebDriver.get();
  } catch {
    await SingletonWebDriver.create({
      browser: process.env.TEST_BROWSER || Browser.CHROME,
      headless: yn(process.env.HEADLESS, { default: true }),
      baseUrl: `http://localhost:${process.env.PORT || 3000}`
    });
  }
});

afterAll(async () => {
  await SingletonWebDriver.quit();
});