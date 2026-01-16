using Antlr4.Runtime;
using CSL.Exceptions;
using CSL.TypeChecker;

namespace CSL.Tests.TypeChecker;


[TestFixture]
public class TypeCheckerVisitorTests
{
    [TestCase("1d")]
    [TestCase("1d + 1d")]
    [TestCase("1d + 1min")]
    [TestCase("1d + 01/01/2001")]
    [TestCase("1d + 10:00")]
    [TestCase("1h + 10:00")]
    public void TestTypeExpressionsValid(string input)
    {
        var stream = CharStreams.fromString(input);
        var lexer = new CSLLexer(stream);
        
        var tokens = new CommonTokenStream(lexer);
        var parser = new CSLParser(tokens);

        var tree = parser.prog();
        var typeVisitor = new TypeCheckerVisitor();
        
        Assert.DoesNotThrow(() => typeVisitor.Visit(tree));
    }

    [TestCase("'abc' + 1d")]
    [TestCase("01/01/2001 + 01/01/2001")]
    public void TestTypeExpressionsInvalid(string input)
    {
        var stream = CharStreams.fromString(input);
        var lexer = new CSLLexer(stream);
        
        var tokens = new CommonTokenStream(lexer);
        var parser = new CSLParser(tokens);

        var tree = parser.prog();
        var typeVisitor = new TypeCheckerVisitor();
        
        Assert.Throws<InvalidTypeCompilerException>(() => typeVisitor.Visit(tree));
    }
}