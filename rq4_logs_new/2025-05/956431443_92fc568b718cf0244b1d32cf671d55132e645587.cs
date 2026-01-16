namespace CSL.Tests;

using CSL;

[TestFixture]
public class CompilerTests
{
    [TestCase("simple-source.txt", "simple-source.result.ics")]
    public void TestSourceCodeExamples(string inputFile, string expectedFile)
    {
        var input = File.ReadAllText(inputFile);

        var compiler = new Compiler();
        var output = compiler.Compile(input);
        
        //var expectedLines = File.ReadAllLines(expectedFile);
        var actualLines = output.Split(Environment.NewLine);
        
        /*for (int i = 0; i < actualLines.Length; i++)
        {
            Assert.That(actualLines[i].Trim(), Is.EqualTo(expectedLines[i].Trim()));
        }*/
    }
}