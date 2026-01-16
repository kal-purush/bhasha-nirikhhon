import { expect, Page } from "@playwright/test";

export class BootstrapAlertsPage {
  readonly page: Page;

  /**
   * BootstrapAlertsPage constructor.
   */
  constructor(page: Page) {
    this.page = page;
  }

  /**
   * Method to check if the BootstrapAlertsPage is visible
   */
  async isReady() {
    expect(this.page.url()).toEqual(
      "https://demo.seleniumeasy.com/bootstrap-alert-messages-demo.html"
    );
    await expect(
      this.page.locator("h2", { hasText: "Bootstrap Alert messages" })
    ).toBeVisible();
  }
}

export default BootstrapAlertsPage;