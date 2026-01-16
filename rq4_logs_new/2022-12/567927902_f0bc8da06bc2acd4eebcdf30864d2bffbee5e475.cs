using System;
using System.Text;

namespace cslox.Parser;

class AstPrinter : IVisitor<string>
{
    public string Print(Expression expression)
    {
        return expression.Accept(this);
    }

    public string VisitBinaryExpression(Binary expression)
    {
        return Parenthesize(expression.Operator.Lexeme, expression.Left, expression.Right);
    }

    public string VisitGroupingExpression(Grouping expression)
    {
        return Parenthesize("group", expression.Expression);
    }

    public string VisitLiteralExpression(Literal expression)
    {
        if (expression.Value == null)
        {
            return "nil";
        }
        return expression.Value.ToString()!;
    }

    public string VisitUnaryExpression(Unary expression)
    {
        return Parenthesize(expression.Operator.Lexeme, expression.Right);
    }

    private string Parenthesize(string name, params Expression[] expressions)
    {
        var builder = new StringBuilder();
        builder.Append("(").Append(name);
        foreach (var expression in expressions)
        {
            builder.Append(" ");
            builder.Append(expression.Accept(this));
        }
        builder.Append(")");

        return builder.ToString();
    }
}
