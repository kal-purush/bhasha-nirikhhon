using CSL.Exceptions;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using System;

namespace CSL
{
    public static class GenericParser
    {
        public static TResult Parse<TResult, TVisitor, TContext>(
            string input,
            Func<TVisitor> visitorFactory,
            Func<CSLParser, TContext> parserRuleSelector,
            Func<TVisitor, TContext, TResult> visitorMethod,
            string entityName = "Input")
            where TContext : ParserRuleContext
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                throw new InvalidLiteralCompilerException($"{entityName}: Input is null or empty");
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
                
                var tree = parserRuleSelector(parser);
                var visitor = visitorFactory();
                return visitorMethod(visitor, tree);
            }
            catch (InvalidLiteralCompilerException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidLiteralCompilerException($"{entityName}: Error parsing input '{input}': {ex.Message}", ex);
            }
        }
    }
    
    public static class ClockParser
    {
        public static Clock ParseClock(string input)
        {
            return GenericParser.Parse<Clock, ClockVisitor, CSLParser.ClockContext>(
                input,
                () => new ClockVisitor(),
                parser => parser.clock(),
                (visitor, tree) => visitor.VisitClock(tree),
                "Clock"
            );
        }
    }
}