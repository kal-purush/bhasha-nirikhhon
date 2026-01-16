/* Licensed under Apache-2.0 2024. */
package github.benslabbert.vertxdaggeriam.fullstack;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import github.benslabbert.vertxdaggeriam.VerticleTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

class FullStackTestBase extends VerticleTestBase {

  private static Playwright playwright;
  private static Browser browser;

  @BeforeAll
  static void launchBrowser() {
    playwright = Playwright.create();
    browser = playwright.chromium().launch();
  }

  @AfterAll
  static void closeBrowser() {
    playwright.close();
  }

  BrowserContext context;
  Page page;

  @BeforeEach
  void createContextAndPage() {
    context = browser.newContext();
    page = context.newPage();
  }

  @AfterEach
  void closeContext() {
    context.close();
  }

  protected void navigateTo(String path) {
    page.navigate("http://127.0.0.1:" + verticle.getPort() + path);
  }
}