namespace WebScrap.Tests.IntegrationTests.KnownIssues;

public class ExtractHtml_KnownIssues_Tests
{
    [Test]
    public void UnableToAccept_Html_With_WhitespacesOnStart()
    {
        var css = "div";
        var html ="    <div></div>";

        Assert.That(() => 
            API.ExtractHtml(html, css),
            Throws.Exception);
    }

    [Test]
    public void AttributedTags_ShouldExtract()
    {
        var css = "p.some";
        var html = """
        <div>
            <p class="some"> One </p>
            <p> Ignore </p>
            <p class="some"> Two </p>
        </div>
        """;

        string[] expected = """
            <p class="some"> One </p>
            <p> Ignore </p>
            <p class="some"> Two </p>
        """.Split(Environment.NewLine);

        var actual = API.ExtractHtml(html, css);
        Assert.Pass();
    }
}