package app;

import static app.TokenType.*;
import java.util.List;

/**
 * Parser
 */
public class Parser {
    private final List<Token> tokens;
    private int current = 0;
    
    private static class ParseError extends RuntimeException {}

    Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    private Token peek() {
        return tokens.get(current);
    }

    private boolean isAtEnd() {
        return peek().type == EOF;
    }

    private boolean check(TokenType type) {
        if (isAtEnd()) {
            return false;
        }
        return peek().type == type;
    }

    private Token previous() {
        return tokens.get(current - 1);
    }

    private Token advance() {
        if (!isAtEnd()) {
            current++;
        }
        return previous();

    }

    private boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) {
                advance();
                return true;
            }
        }
        return false;
    }
    private ParseError error(Token token, String message) {
        MyLox.error(token, message);
        return new ParseError();
    }
    private Expr primary() {
        // primary  → NUMBER | STRING | "false" | "true" | "nil"
        //          | "(" expression ")" ;
        if(match(FALSE)) {
            return new Expr.Literal(false);
        }
        
        if(match(TRUE)) {
            return new Expr.Literal(true);            
        }
        
        if(match(NIL)) {
            return new Expr.Literal(null);            
        }

        if(match(STRING, NUMBER)) {
            Token token = previous();
            return new Expr.Literal(token.literal);
        }
        
        if(match(LEFT_PAREN)) {
            // 可能是左括号
            Expr expr = expression();
            if(match(RIGHT_PAREN)) {
                return new Expr.Grouping(expr);
            } else {
                throw error(peek(), "Expect ')' after expression.");
            }
        } else {
            throw error(peek(), "Expect expression.");
        }

    }

    private Expr unary() {
        // unary → ( "!" | "-" ) unary
        //       | primary ;
        if(match(BANG, MINUS)) {
            Token operator = previous();
            Expr right = unary();
            return new Expr.Unary(operator, right);
        }

        return primary();
    }

    private Expr multiplication() {
        // multiplication → unary ( ( "/" | "*" ) unary )* ;
        Expr expr = unary();

        if(match(SLASH, STAR)) {
            Token operator = previous();
            Expr right = unary();
            return new Expr.Binary(expr, operator, right);
        }

        return expr;
    }

    private Expr addition() {
        // addition → multiplication ( ( "-" | "+" ) multiplication )*;
        Expr expr = multiplication();

        if(match(MINUS, PLUS)) {
            Token operator = previous();
            Expr right = multiplication();
            return new Expr.Binary(expr, operator, right);
        }

        return expr;
    }

    private Expr comparison() {
        // comparison → addition ( ( ">" | ">=" | "<" | "<=" ) addition )* ;
        Expr expr = addition();

        if(match(GREATER, GREATER_EQUAL, LESS, LESS_EQUAL)) {
            Token operator = previous();
            Expr right = addition();
            expr = new Expr.Binary(expr, operator, right);
        }

        return expr;
    }

    private Expr equality() {
        // equality → comparison ( ( "!=" | "==" ) comparison )* ;
        Expr expr = comparison();

        if (match(BANG_EQUAL, EQUAL_EQUAL)) {
            Token operator = previous();
            Expr right = comparison();
            expr = new Expr.Binary(expr, operator, right);
            return expr;
        }

        return expr;
    }

    private Expr expression() {
        // expression → equality ;
        return equality();
    }

    Expr parse() {
        try {
            return expression();
        } catch (ParseError error) {
            return null;
        }
    }

}