using CSL.Exceptions;
using Antlr4.Runtime;
using System;

namespace CSL
{
    public static class ClockParser
    {
        public static Clock ParseClock(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                throw new InvalidLiteralCompilerException("Clock: Input is null or empty");
            }

            try
            {
                var stream = CharStreams.fromString(input);
                var lexer = new CSLLexer(stream);
                lexer.RemoveErrorListeners();
                lexer.AddErrorListener(new CSLLexerErrorListener());
                
                var tokens = new CommonTokenStream(lexer);
                var parser = new CSLParser(tokens);
                parser.RemoveErrorListeners();
                parser.AddErrorListener(new CSLParserErrorListener());
                
                var tree = parser.clock();
                var visitor = new ClockVisitor();
                return visitor.VisitClock(tree);
            }
            catch (InvalidLiteralCompilerException)
            {
                // Re-throw our custom exceptions
                throw;
            }
            catch (Exception ex)
            {
                // Convert any other exception to our custom type
                throw new InvalidLiteralCompilerException($"Clock: Error parsing input '{input}': {ex.Message}", ex);
            }
        }
    }
}