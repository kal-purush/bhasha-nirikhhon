using Atmoos.Sphere.Text;

using static Atmoos.Sphere.Text.LineMarks;

namespace Atmoos.Sphere.Test.Text;

public class TextInsertTest
{
    [Fact]
    public void InsertText_ShouldInsertTextAtSpecifiedIndex()
    {
        var mark = Markdown.Code("text");
        var toReplace = """
                        Hi there!
                        This should be replaced.
                        """;
        var toInsert = """
                        This is new text!
                        And more lines
                        than before :-)
                        """;
        var source = new StringReader(Text(mark, toReplace));
        var destination = new StringWriter();

        destination.InsertText(source, mark, toInsert.Split(Environment.NewLine));

        var expected = Text(mark, toInsert);
        var actual = destination.ToString();

        Assert.Equal(expected, actual);
    }

    static String Text(in LineMark tag, String insert)
    {
        return $"""
                # Sample
                Hello World!
                ## Sub Sample
                This be intro.
                {tag}
                {insert}
                {tag.Tag}
                and this be outro.
                
                """;
    }
}