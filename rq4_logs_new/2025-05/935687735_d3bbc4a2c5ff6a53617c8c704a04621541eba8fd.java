package ru.itmo.icompiler.syntax.ast;

import ru.itmo.icompiler.lex.Token;
import ru.itmo.icompiler.semantic.visitor.ASTVisitor;

public class ContinueStatementASTNode extends ASTNode {
	private LoopStatementASTNode loopNode;
	private Token token;

	public ContinueStatementASTNode(ASTNode parentNode, Token token) {
		super(parentNode, ASTNodeType.CONTINUE_STMT_NODE);
		
		this.token = token;
	}
	
	public void setLoopNode(LoopStatementASTNode loopNode) {
		this.loopNode = loopNode;
	}
	
	public LoopStatementASTNode getLoopNode() {
		return loopNode;
	}
	
	public Token getToken() {
		return token;
	}
	
	public<R, A> R accept(ASTVisitor<R, A> visitor, A arg) {
		return visitor.visit(this, arg);
	}
}