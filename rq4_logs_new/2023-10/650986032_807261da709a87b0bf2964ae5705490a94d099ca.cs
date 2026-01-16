namespace WebScrap.API.KnownIssues.Tests;

public class CssSelectors_KnownIssues_Tests
{
    [TestCase("*", Description="*", Category=Categories.KnownIssues)]
    [TestCase("div>*", Description="*", Category=Categories.KnownIssues)]
    [TestCase("*>div", Description="*", Category=Categories.KnownIssues)]
    [TestCase("div *", Description="*", Category=Categories.KnownIssues)]
    [TestCase("* div", Description="*", Category=Categories.KnownIssues)]
    [TestCase("[data]", Description="[]", Category=Categories.KnownIssues)]
    [TestCase(".some.class, .some.other.class", Description=",", Category=Categories.KnownIssues)]
    public void ExtractHtml_FailsToDetect_SomeCss(string css)
    {
        var html = "<div> LoremIpsum </div>";
        Assert.That(() => Extract.Html(html, css), Throws.Exception);
    }
}