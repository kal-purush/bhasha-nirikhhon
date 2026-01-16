package Statements;

import Expressions.ASTNode;

import java.util.ArrayList;
import java.util.List;
import Compiler.Token;
import Expressions.ShuntingYardParser;

public class ElseIfStatement implements Statement {
    ASTNode condition;
    List<Statement> statements;
    ShuntingYardParser parser;

    public ElseIfStatement(List<Token> expression, List<Statement> statements) {
        this.parser = new ShuntingYardParser();

        this.condition = parser.convertInfixNotationToAST(expression);
        this.statements = statements;
    }
}