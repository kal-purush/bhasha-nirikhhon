using System;
using cslox.Scanning;

namespace cslox.Parser;

class Parser
{
    private readonly List<Token> tokens;
    private int current = 0;

    public Parser(List<Token> tokens)
    {
        this.tokens = tokens;
    }

    public Expression? Parse()
    {
        try
        {
            return Expression();
        }
        catch (ParseError)
        {
            return null;
        }
    }

    private Expression Expression()
    {
        return Equality();
    }

    private Token Peek() => tokens[current];
    private Token Previous() => tokens[current - 1];

    private bool IsAtEnd() => Peek().Type == TokenType.EOF;

    private Token Advance()
    {
        if (!IsAtEnd())
        {
            current++;
        }

        return Previous();
    }

    private bool Check(TokenType tokenType)
    {
        if (IsAtEnd())
        {
            return false;
        }
        return Peek().Type == tokenType;
    }

    private bool Match(params TokenType[] tokenTypes)
    {
        foreach (var tokenType in tokenTypes)
        {
            if (Check(tokenType))
            {
                Advance();
                return true;
            }
        }

        return false;
    }

    // equality       → comparison ( ( "!=" | "==" ) comparison )* ;
    private Expression Equality()
    {
        var expr = Comparison();

        while (Match(TokenType.BANG_EQUAL, TokenType.EQUAL_EQUAL))
        {
            var @operator = Previous();
            var right = Comparison();
            expr = new Binary(expr, @operator, right);
        }

        return expr;
    }

    // comparison     → term(( ">" | ">=" | "<" | "<=" ) term )* ;
    private Expression Comparison()
    {
        var expr = Term();

        while (Match(TokenType.GREATER, TokenType.GREATER_EQUAL, TokenType.LESS, TokenType.LESS_EQUAL))
        {
            var @operator = Previous();
            var right = Term();
            expr = new Binary(expr, @operator, right);
        }

        return expr;
    }

    // term           → factor ( ( "-" | "+" ) factor )* ;
    private Expression Term()
    {
        var expr = Factor();

        while (Match(TokenType.MINUS, TokenType.PLUS))
        {
            var @operator = Previous();
            var right = Factor();
            expr = new Binary(expr, @operator, right);
        }

        return expr;
    }

    // factor         → unary ( ( "/" | "*" ) unary )* ;
    private Expression Factor()
    {
        var expr = Unary();

        while (Match(TokenType.SLASH, TokenType.STAR))
        {
            var @operator = Previous();
            var right = Unary();
            expr = new Binary(expr, @operator, right);
        }

        return expr;
    }

    // unary          → ( "!" | "-" ) unary
    //                | primary ;
    private Expression Unary()
    {
        if (Match(TokenType.BANG, TokenType.MINUS))
        {
            var @operator = Previous();
            var right = Unary();
            return new Unary(@operator, right);
        }

        return Primary();
    }

    // primary        → NUMBER | STRING | "true" | "false" | "nil"
    //                | "(" expression ")" ;
    private Expression Primary()
    {
        if (Match(TokenType.FALSE))
        {
            return new Literal(false);
        }
        if (Match(TokenType.TRUE))
        {
            return new Literal(true);
        }
        if (Match(TokenType.NIL))
        {
            return new Literal(null);
        }

        if (Match(TokenType.NUMBER, TokenType.STRING))
        {
            return new Literal(Previous().Literal);
        }

        if (Match(TokenType.LEFT_PAREN))
        {
            var expr = Expression();
            Consume(TokenType.RIGHT_PAREN, "Expect ')' after expression.");
            return new Grouping(expr);
        }

        throw Error(Peek(), "Expect expression.");
    }

    private Token Consume(TokenType tokenType, string message)
    {
        if (Check(tokenType))
        {
            return Advance();
        }

        throw Error(Peek(), message);
    }

    private ParseError Error(Token token, string message)
    {
        Lox.Error(token, message);
        return new ParseError();
    }

    private void Synchronize()
    {
        Advance();

        while (!IsAtEnd())
        {
            if (Previous().Type == TokenType.SEMICOLON)
            {
                return;
            }

            switch (Peek().Type)
            {
                case TokenType.CLASS:
                case TokenType.FUN:
                case TokenType.VAR:
                case TokenType.FOR:
                case TokenType.IF:
                case TokenType.WHILE:
                case TokenType.PRINT:
                case TokenType.RETURN:
                    return;
            }
        }

        Advance();
    }
}

class ParseError : Exception
{
}