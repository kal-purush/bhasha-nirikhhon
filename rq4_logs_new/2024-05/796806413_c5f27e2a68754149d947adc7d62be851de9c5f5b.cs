using Atmoos.Sphere.Text;

using static Atmoos.Sphere.Text.LineMarks;

namespace Atmoos.Sphere.Test.Text;

public class LineMarkTest
{
    [Fact]
    public void ToStringConcatenatesTagAndName()
    {
        var mark = new LineMark { Tag = "Foo", Name = "Bar" };
        Assert.Equal("FooBar", mark.ToString());
    }

    [Fact]
    public void MarkdownHeaderIsCorrect()
    {
        var header = Markdown.Header("SomeName");
        var expected = new LineMark { Tag = "#", Name = " SomeName" };
        Assert.Equal(expected, header);
    }

    [Fact]
    public void MarkdownSubHeaderIsCorrect()
    {
        var subHeader = Markdown.SubHeader("SomeName");
        var expected = new LineMark { Tag = "##", Name = " SomeName" };
        Assert.Equal(expected, subHeader);
    }

    [Fact]
    public void MarkdownSubSubHeaderIsCorrect()
    {
        var subSubHeader = Markdown.SubSubHeader("SomeName");
        var expected = new LineMark { Tag = "###", Name = " SomeName" };
        Assert.Equal(expected, subSubHeader);
    }

    [Fact]
    public void MarkdownCodeIsCorrect()
    {
        var code = Markdown.Code("SomeName");
        var expected = new LineMark { Tag = "```", Name = "SomeName" };
        Assert.Equal(expected, code);
    }
}