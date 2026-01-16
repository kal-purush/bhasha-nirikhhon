import { test as setup, expect } from "@playwright/test";
import { login } from "./login";

const adminFile = "playwright/.auth/admin.json";

setup("authenticate as admin", async ({ page }) => {
  await login(page, "delorang28@gmail.com", "abekat123456");
  const headerElement = page.locator(
    'header div div div:has-text("goeasytestadmin")'
  );
  const headerText = await headerElement.innerText();
  expect(headerText).toContain("goeasytestadmin");
  await page.context().storageState({ path: adminFile });
});

const userFile = "playwright/.auth/user.json";

setup("authenticate as user", async ({ page }) => {
  await login(page, "galvit28@gmail.com", "abekat123456");
  const headerElement = page.locator(
    'header div div div:has-text("Go Easy Test")'
  );
  const headerText = await headerElement.innerText();
  expect(headerText).toContain("Go Easy Test");
  await page.context().storageState({ path: userFile });
});