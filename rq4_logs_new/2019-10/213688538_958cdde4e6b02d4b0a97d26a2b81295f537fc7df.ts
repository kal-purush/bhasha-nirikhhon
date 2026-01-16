import { DefineStepFunction } from "jest-cucumber";

import SingletonWebDriver from "../webdriver/SingletonWebDriver";

export const thenThePageTitleShouldBeX = (then: DefineStepFunction): void => {
  then(/^the page title should be "(.+)"$/, async (title: string) => {
    const driver = SingletonWebDriver.get();

    await expect(driver.getTitle()).resolves.toEqual(title);
  });
};

export const thenIShouldSeeXOnThePage = (then: DefineStepFunction): void => {
  then(/^I should see "(.+)" on the page$/, async (content: string) => {
    const driver = SingletonWebDriver.get();

    const element = await driver.findElement({ css: "[data-test=content]" });

    await expect(element.getText()).resolves.toContain(content);
  });
};