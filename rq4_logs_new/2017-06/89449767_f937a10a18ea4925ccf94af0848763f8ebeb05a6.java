package Statements;

import Compiler.Token;
import Expressions.ASTNode;
import Expressions.ShuntingYardParser;

import java.util.List;

public class ForStatement implements Statement {
    ASTNode assignExpression;
    ASTNode condition;
    ASTNode cycleExpression;
    List<Statement> cycleStatements;
    ShuntingYardParser parser;

    public ForStatement(List<Token> assignExpression, List<Token> conditionExpression) {
        this.parser = new ShuntingYardParser();

        this.assignExpression = parser.convertInfixNotationToAST(assignExpression);
        this.condition = parser.convertInfixNotationToAST(conditionExpression);
    }

    public void setCycleExpression(List<Token> cycleExpression) {
        this.cycleExpression = parser.convertInfixNotationToAST(cycleExpression);
    }

    public void setCycleStatements(List<Statement> statements) {
        this.cycleStatements = statements;
    }
}