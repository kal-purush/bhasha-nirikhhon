namespace WebScrap.API.Tests.Features;

[TestFixture(Category=Categories.Features)]
public class ExtractWildcardTests
{
    [Test]
    public void Test1()
    {
        var css = "*.bar";
        var html ="""
        <main>
            <div>
                <p> <div> <span class="bar"> One </span> </div> </p>
                <p> <div class="bar"> Two <span class="bar"> One </span> </div> </p>
                <p>
                <ul>
                    <li> <span> One </span> </li>
                    <li> <span class="bar"> Two </span> </li>
                    <li> <span class="bar buzz"> Three </span> </li>
                </ul>
                </p>
            </div>
        </main>
        """;

        string[] expected = [
            """<span class="bar"> One </span>""",
            """<span class="bar"> One </span>""",
            """<div class="bar"> Two <span class="bar"> One </span> </div>""",
            """<span class="bar"> Two </span>""",
            """<span class="bar buzz"> Three </span>"""
        ];

        var actual = Extract.Html(html, css);
        Assert.That(actual, Is.EquivalentTo(expected));
    }
}