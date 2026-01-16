namespace CSL.CLI;

using Antlr4.Runtime;
using System;
using System.IO;

class Program
{
    static void Main(string[] args)
    {
        string input;
            
        // Check if an argument was provided
        if (args.Length > 0)
        {
            // Use the first command-line argument as input
            input = args[0];
        }
        else
        {
            // Default input if no arguments provided
            input = "";
            Console.WriteLine("No input provided, using default: " + input);
        }

        ICharStream stream = CharStreams.fromString(input);
            
        var lexer = new CSLLexer(stream);
        var tokens = new CommonTokenStream(lexer);
        var parser = new CSLParser(tokens);
            
        var tree = parser.prog();
            
        // Create and use the visitor
        var visitor = new CSLCustomVisitor();
        string result = visitor.Visit(tree);
            
        Console.WriteLine($"Input: {input}");
        Console.WriteLine($"Result: {result}");
    }
}