import { Expr } from "./expr";
import { Lox } from "./lox";
import { Token } from "./token";
import { TokenType } from "./tokenType";

export class Parser {
  private tokens: Token[];
  private current: number = 0;

  constructor(tokens: Token[]) {
    this.tokens = tokens;
  }

  public parse(): Expr.Expr | null {
    try {
      return this.expression();
    } catch (error) {
      if (error instanceof ParseError) {
        return null;
      }
      throw error;
    }
  }

  private expression(): Expr.Expr {
    return this.equality();
  }

  private equality(): Expr.Expr {
    let expr: Expr.Expr = this.comparison();

    while (this.match("BANG_EQUAL", "EQUAL_EQUAL")) {
      const operator: Token = this.previous();
      const right: Expr.Expr = this.comparison();
      expr = new Expr.Binary(expr, operator, right);
    }

    return expr;
  }

  private comparison(): Expr.Expr {
    let expr: Expr.Expr = this.term();

    while (this.match("GREATER", "GREATER_EQUAL", "LESS", "LESS_EQUAL")) {
      const operator: Token = this.previous();
      const right: Expr.Expr = this.term();
      expr = new Expr.Binary(expr, operator, right);
    }
    return expr;
  }

  private term(): Expr.Expr {
    let expr: Expr.Expr = this.factor();
    while (this.match("MINUS", "PLUS")) {
      const operator: Token = this.previous();
      const right: Expr.Expr = this.factor();
      expr = new Expr.Binary(expr, operator, right);
    }
    return expr;
  }

  private factor(): Expr.Expr {
    let expr: Expr.Expr = this.unary();
    while (this.match("SLASH", "STAR")) {
      const operator: Token = this.previous();
      const right: Expr.Expr = this.unary();
      expr = new Expr.Binary(expr, operator, right);
    }
    return expr;
  }

  private unary(): Expr.Expr {
    if (this.match("BANG", "MINUS")) {
      const operator: Token = this.previous();
      const right: Expr.Expr = this.unary();
      return new Expr.Unary(operator, right);
    }
    return this.primary();
  }

  private primary(): Expr.Expr {
    if (this.match("FALSE")) {
      return new Expr.Literal(false);
    }

    if (this.match("TRUE")) {
      return new Expr.Literal(true);
    }

    if (this.match("NIL")) {
      return new Expr.Literal(null);
    }

    if (this.match("NUMBER", "STRING")) {
      return new Expr.Literal(this.previous().literal);
    }

    if (this.match("LEFT_PARENS")) {
      const expr: Expr.Expr = this.expression();
      this.consume("RIGHT_PARENS", "Expect ')' after expression.");
      return new Expr.Grouping(expr);
    }

    throw this.error(this.peek(), "Expect expression.");
  }

  /**
   * If the current token is of one of the given types, return true and consume it.
   */
  private match(...types: TokenType[]): boolean {
    for (const type of types) {
      if (this.check(type)) {
        this.advance();
        return true;
      }
    }

    return false;
  }

  /**
   * If the current token has the given type, consume it and return true.
   * Otherwise report an error.
   */
  private consume(type: TokenType, message: string): Token {
    if (this.check(type)) {
      return this.advance();
    }
    throw this.error(this.peek(), message);
  }

  /**
   * Report a parse error.
   */
  private error(token: Token, message: string): Error {
    Lox.error(token, message);
    return new ParseError();
  }

  private synchronize(): void {
    this.advance();

    while (!this.isAtEnd()) {
      if (this.previous().type == "SEMICOLON") {
        return;
      }

      switch (this.peek().type) {
        case "CLASS":
        case "FUN":
        case "VAR":
        case "FOR":
        case "IF":
        case "WHILE":
        case "PRINT":
        case "RETURN":
          return;
      }

      this.advance();
    }
  }

  /**
   * Return true if the current token has the given type.
   */
  private check(type: TokenType): boolean {
    if (this.isAtEnd()) {
      return false;
    }
    return this.peek().type == type;
  }

  /**
   * Consume the current tokena and return it.
   */
  private advance(): Token {
    if (!this.isAtEnd()) {
      this.current++;
    }
    return this.previous();
  }

  /**
   * Return true if there are no more tokens to consume.
   */
  private isAtEnd(): boolean {
    return this.peek().type == "EOF";
  }

  /**
   * Return the current token we have not yet consumed.
   */
  private peek(): Token {
    return this.tokens[this.current];
  }

  /**
   * Return the most recently consumed token.
   */
  private previous(): Token {
    return this.tokens[this.current - 1];
  }
}

class ParseError extends Error {}