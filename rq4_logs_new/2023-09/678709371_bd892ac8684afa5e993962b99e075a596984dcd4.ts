import { expect, test } from "@playwright/test";

test.describe("Go Easy @access @admin", () => {
  test.use({ storageState: "./playwright/.auth/admin.json" });
  test("change access from pending to granted and back to pending", async ({ page }) => {
    await page.goto("http://localhost:3000");
    await page.getByRole("button", { name: "Anmodninger" }).click();
    await page
      .locator(
        'tr:has-text("Go Easy Test") td[test-column="access"] input:not([type="hidden"])'
      )
      .click();
    await page.getByRole("option", { name: "Granted" }).click();
    await page.waitForURL("/requests");
    await page.goto("/advertisers");
    const cellElement = page.locator('tr:has-text("Go Easy Test")');
    await expect(cellElement).toBeVisible();
    await page
      .locator(
        'tr:has-text("Go Easy Test") td[test-column="access"] input:not([type="hidden"])'
      )
      .click();
    await page.getByRole("option", { name: "Pending" }).click();
    await page.waitForURL("/advertisers");
    await expect(cellElement).toBeHidden();
  });
});