package com.auberer.compilerdesignlectureproject.ast;

import com.auberer.compilerdesignlectureproject.lexer.TokenType;

import java.util.HashSet;
import java.util.Set;

public class ASTArgLstNode extends ASTNode {
    @Override
    public <T> T accept(ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public static Set<TokenType> getSelectionSet() {
        Set<TokenType> selectionSet = new HashSet<TokenType>();
        selectionSet.addAll(ASTLiteralNode.getSelectionSet());
        selectionSet.addAll(ASTFunctionCallNode.getSelectionSet());
        selectionSet.addAll(ASTPrintBuiltinCallNode.getSelectionSet());
        selectionSet.addAll(ASTLiteralNode.getSelectionSet());
        selectionSet.add(TokenType.TOK_LPAREN);
        return selectionSet;
    }
}