using System;
using System.Collections.Generic;

namespace Interpreter.Structural
{
    /// <summary>
    /// Interpreter Design Pattern
    /// </summary>

    public class Program
    {
        public static void Main(string[] args)
        {
            Context context = new Context();

            // Usually a tree 

            List<AbstractExpression> list = new List<AbstractExpression>();

            // Populate 'abstract syntax tree' 

            list.Add(new TerminalExpression());
            list.Add(new NonterminalExpression());
            list.Add(new TerminalExpression());
            list.Add(new TerminalExpression());

            // Interpret

            foreach (AbstractExpression exp in list)
            {
                exp.Interpret(context);
            }

            // Wait for user

            Console.ReadKey();
        }
    }

    /// <summary>
    /// The 'Context' class
    /// </summary>

    public class Context
    {
    }

    /// <summary>
    /// The 'AbstractExpression' abstract class
    /// </summary>

    public abstract class AbstractExpression
    {
        public abstract void Interpret(Context context);
    }

    /// <summary>
    /// The 'TerminalExpression' class
    /// </summary>

    public class TerminalExpression : AbstractExpression
    {
        public override void Interpret(Context context)
        {
            Console.WriteLine("Called Terminal.Interpret()");
        }
    }

    /// <summary>
    /// The 'NonterminalExpression' class
    /// </summary>

    public class NonterminalExpression : AbstractExpression
    {
        public override void Interpret(Context context)
        {
            Console.WriteLine("Called Nonterminal.Interpret()");
        }
    }
}