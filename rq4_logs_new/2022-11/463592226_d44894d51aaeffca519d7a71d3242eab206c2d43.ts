import { faker } from "@faker-js/faker";
import { test } from "@playwright/test";

test.describe("smoke tests", () => {
  test("should allow you to register and login", async ({ page }) => {
    const loginForm = {
      email: `${faker.internet.userName()}@example.com`,
      password: faker.internet.password(),
    };
    await page.goto("http://localhost:3000/login");
    await page.click("text=Sign up");
    await page.fill('input[name="email"]', loginForm.email);
    await page.fill('input[name="password"]', loginForm.password);
    await page.click("text=Create account");
    await page.click('img[alt="placeholder"]');
    await page.click("text=Favorites");
    await page.click("text=Imported");
    await page.click("text=Logout");
  });

  test("should allow you to favorite and unfavorite a recipe", async ({ page }) => {
    await page.goto("http://localhost:3000/");
    await page.click("text=Add to favorites");
  });
});