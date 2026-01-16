// Generated from ANB.g4 by ANTLR 4.7
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ANBParser}.
 */
public interface ANBListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Document}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Document(ANBParser.Anb_DocumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Document}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Document(ANBParser.Anb_DocumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Protocol}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Protocol(ANBParser.Anb_ProtocolContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Protocol}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Protocol(ANBParser.Anb_ProtocolContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Types}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Types(ANBParser.Anb_TypesContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Types}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Types(ANBParser.Anb_TypesContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_type}.
	 * @param ctx the parse tree
	 */
	void enterAnb_type(ANBParser.Anb_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_type}.
	 * @param ctx the parse tree
	 */
	void exitAnb_type(ANBParser.Anb_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Knowlegde}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Knowlegde(ANBParser.Anb_KnowlegdeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Knowlegde}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Knowlegde(ANBParser.Anb_KnowlegdeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Know}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Know(ANBParser.Anb_KnowContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Know}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Know(ANBParser.Anb_KnowContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Actions}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Actions(ANBParser.Anb_ActionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Actions}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Actions(ANBParser.Anb_ActionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Action}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Action(ANBParser.Anb_ActionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Action}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Action(ANBParser.Anb_ActionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_Goals}.
	 * @param ctx the parse tree
	 */
	void enterAnb_Goals(ANBParser.Anb_GoalsContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_Goals}.
	 * @param ctx the parse tree
	 */
	void exitAnb_Goals(ANBParser.Anb_GoalsContext ctx);
	/**
	 * Enter a parse tree produced by {@link ANBParser#anb_goal}.
	 * @param ctx the parse tree
	 */
	void enterAnb_goal(ANBParser.Anb_goalContext ctx);
	/**
	 * Exit a parse tree produced by {@link ANBParser#anb_goal}.
	 * @param ctx the parse tree
	 */
	void exitAnb_goal(ANBParser.Anb_goalContext ctx);
}