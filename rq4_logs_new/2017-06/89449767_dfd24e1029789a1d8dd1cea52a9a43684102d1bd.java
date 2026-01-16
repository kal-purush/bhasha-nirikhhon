package Statements;

import Expressions.ASTNode;
import Compiler.Variable;
import Compiler.Token;
import Expressions.ShuntingYardParser;

import java.util.ArrayList;
import java.util.List;

public class AssignStatement implements Statement {
    ASTNode syntaxTree;
    ShuntingYardParser parser;

    public AssignStatement(Variable variable, List<Token> expression) {
        parser = new ShuntingYardParser();

        List<Token> assign = new ArrayList<>();
        assign.add(variable.getToken());
        assign.add(new Token("=", 115, 0));
        assign.addAll(expression);

        this.syntaxTree = parser.convertInfixNotationToAST(assign);
    }

}