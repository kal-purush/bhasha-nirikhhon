namespace WebScrap.API.Tests.Features;

[TestFixture(Category=Categories.Features)]
public class DeepChildLocationTests
{
    [Test]
    public void Test1()
    {
        var css = "div.container b";
        var html = """
        <main>
            <div class='container'>
                <p> LoremIpsum </p>
                <p> Lorem <b> Ipsum </b> </p>
                <b> Important </b>
            </div>
        </main>
        """;

        string[] expected = [
            "<b> Ipsum </b>",
            "<b> Important </b>"];
        
        var actual = Extract.Html(html, css);
        Assert.That(actual, Is.EquivalentTo(expected));
    }

    [Test]
    public void Test2()
    {
        var css ="div.container b";
        var html = """
        <main class="container">
            <div class="container">
                <p> Lorem <b> Ipsum </b> </p>
            </div>
            <div>
                <p> Lorem <b> Ipsum </b> </p>
            </div>   
        </main>
        """;

        string[] expected = [
            "<b> Ipsum </b>",
            "<b> Ipsum </b>"];

        var actual = Extract.Html(html, css);
        Assert.That(actual, Is.EquivalentTo(expected));
    }

    [Test]
    public void Test3()
    {
        var css ="main.container b";
        var html = """
        <main class="container">
            <div class="container">
                <p> Lorem <b> Ipsum </b> </p>
                <b> Clatu <b> Barata <b> Nictu </b> </b> </b>
            </div>
        </main>
        """;

        string[] expected = [
            "<b> Ipsum </b>",
            "<b> Clatu <b> Barata <b> Nictu </b> </b> </b>",
            "<b> Barata <b> Nictu </b> </b>",
            "<b> Nictu </b>"];

        var actual = Extract.Html(html, css);
        Assert.That(actual, Is.EquivalentTo(expected));
    }
}