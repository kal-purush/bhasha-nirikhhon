namespace Chirp.Web.End2EndTests;

[Parallelizable(ParallelScope.Self)]
[TestFixture]
public class CreateCheepTests : PageTest
{
    [Test]
    public async Task AuthenticatedUserCanCreateCheepFromPublicTimeline()
    {
        await using var browser = await Playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
        {
            Headless = false
        });

        var context = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            IgnoreHTTPSErrors = true,
        });

        var page = await context.NewPageAsync();

        await page.GotoAsync("https://localhost:7102/");

        await page.GetByRole(AriaRole.Link, new() { Name = "login" }).ClickAsync();

        await page.WaitForURLAsync("https://localhost:7102/", new PageWaitForURLOptions() { Timeout = 0 });

        await page.Locator("#CheepMessage").ClickAsync();

        await page.Locator("#CheepMessage").FillAsync("Testing");

        await page.GetByRole(AriaRole.Button, new() { Name = "Share" }).ClickAsync();

        await Expect(page.Locator("#messagelist")).ToContainTextAsync("Testing");
    }
}