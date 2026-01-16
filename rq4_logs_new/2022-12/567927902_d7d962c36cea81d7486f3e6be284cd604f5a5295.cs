using cslox.Parser;
using cslox.Scanning;

namespace cslox.Interpreter;

class Interpreter : IVisitor<object?>
{
    public void Interpret(Expression expression)
    {
        try
        {
            var value = Evaluate(expression);
            Console.WriteLine(Stringify(value));
        }
        catch (RuntimeError error)
        {
            Lox.RuntimeError(error);
        }
    }

    public object? VisitBinaryExpression(Binary expression)
    {
        var left = Evaluate(expression.Left);
        var right = Evaluate(expression.Right);

        switch (expression.Operator.Type)
        {
            case TokenType.GREATER:
                CheckNumberOperands(expression.Operator, left, right);
                return (double)left! > (double)right!;
            case TokenType.GREATER_EQUAL:
                CheckNumberOperands(expression.Operator, left, right);
                return (double)left! >= (double)right!;
            case TokenType.LESS:
                CheckNumberOperands(expression.Operator, left, right);
                return (double)left! < (double)right!;
            case TokenType.LESS_EQUAL:
                CheckNumberOperands(expression.Operator, left, right);
                return (double)left! <= (double)right!;
            case TokenType.BANG_EQUAL:
                return !IsEqual(left, right);
            case TokenType.EQUAL_EQUAL:
                return IsEqual(left, right);
            case TokenType.MINUS:
                CheckNumberOperands(expression.Operator, left, right);
                return (double) left! - (double)right!;
            case TokenType.PLUS:
                if (left is double && right is double)
                {
                    return (double)left! + (double)right!;
                }

                if (left is string && right is string)
                {
                    return (string)left + (string)right;
                }
                throw new RuntimeError(expression.Operator, "Operands must be two numbers or two strings");
            case TokenType.SLASH:
                CheckNumberOperands(expression.Operator, left, right);
                if ((double)right! == 0)
                {
                    throw new RuntimeError(expression.Operator, "Division by zero");
                }
                return (double)left! / (double)right!;
            case TokenType.STAR:
                CheckNumberOperands(expression.Operator, left, right);
                return (double)left! * (double)right!;

        }

        return null;
    }

    public object? VisitGroupingExpression(Grouping expression)
    {
        return Evaluate(expression.Expression);
    }

    public object? VisitLiteralExpression(Literal expression)
    {
        return expression.Value;
    }

    public object? VisitUnaryExpression(Unary expression)
    {
        var right = Evaluate(expression.Right);
        switch (expression.Operator.Type)
        {
            case TokenType.BANG:
                return !IsTruthy(right);
            case TokenType.MINUS:
                CheckNumberOperand(expression.Operator, right);
                return -(double)right!;
        }

        return null;
    }

    private object? Evaluate(Expression expression)
    {
        return expression.Accept(this);
    }

    private bool IsTruthy(object? obj)
    {
        if (obj == null)
        {
            return false;
        }

        if (obj is bool)
        {
            return (bool)obj;
        }

        return true;
    }

    private bool IsEqual(object? a, object? b)
    {
        if (a == null && b == null)
        {
            return true;
        }
        if (a == null)
        {
            return false;
        }

        return a.Equals(b);
    }

    private void CheckNumberOperand(Token @operator, object? operand)
    {
        if (operand is double)
        {
            return;
        }

        throw new RuntimeError(@operator, "Operand must be a number.");
    }

    private void CheckNumberOperands(Token @operator, object? left, object? right)
    {
        if (left is double && right is double)
        {
            return;
        }

        throw new RuntimeError(@operator, "Operands must be numbers.");
    }

    private string Stringify(object? value)
    {
        if (value == null)
        {
            return "nil";
        }

        if (value is double)
        {
            var text = ((double)value).ToString();
            if (text.EndsWith(".0"))
            {
                text = text.Substring(0, text.Length - 2);
            }
            return text;
        }

        return value.ToString()!;
    }
}

class RuntimeError : Exception
{
    public Token Token { get; }

    public RuntimeError(Token token, string message)
        : base(message)
    {
        Token = token;
    }
}