package com.pilers.ast;

import com.pilers.emitter.Emitter;
import com.pilers.environment.*;
import com.pilers.errors.ErrorString;
import com.pilers.errors.InterpretException;
import com.pilers.errors.CompileException;

/**
 * AST Assignment class
 * Represents the assignment of a value to a variable
 * 
 * @author Gloria Zhu
 */
public class Assignment extends Statement
{
    private String var;
    private Expression exp;

    /**
     * Constructs an Assignment object
     * 
     * @param var the name of the variable
     * @param exp an expression for the value
     * @throws TypeErrorException
     */
    public Assignment(String var, Expression exp)
    {
        this.var = var;
        this.exp = exp;
    }

    /**
     * Executes the assignment
     * 
     * @param env the execution environment (InterpreterEnvironment)
     * @throws BreakException    if a break statement is executed
     * @throws ContinueException if a continue statement is executed
     * @throws InterpretException if there is a runtime error in assignment
     *                          Happens if the varaible does not exist,
     *                          or there is a type discrepancy between the
     *                          variable and value
     */
    public void exec(InterpreterEnvironment env) 
        throws BreakException, ContinueException, InterpretException
    {
        env.setVariable(var, exp.eval(env));
    }

    /**
     * Semantic analysis for assignment
     * Checks if the variable exists and the types are compatible
     * 
     * @throws CompileException if the conditions above are broken
     * @param env the current environment
     */
    public void analyze(SemanticAnalysisEnvironment env) throws CompileException
    {
        exp.analyze(env);

        String expectedType = env.getVariableType(var);

        if (expectedType == null) throw new CompileException(ErrorString.unknownIdentifier(var));

        if (!expectedType.equals(exp.type))
            throw new CompileException(ErrorString.typeAssignment(expectedType, exp.type));
    }

    /**
     * Compiles the assignment
     * The specific depends on the type of the varaible
     * 
     * @param e the emitter
     */
    public void compile(Emitter e)
    {
        exp.compile(e); // most recent value is in v0

        if (e.isGlobalVariable(var)) e.emit("sw $v0 "+"_data_"+var);
        else
        {
            int index = e.getLocalVarIndex(var);
            e.emit("sw $v0 " + index * 4 + "($sp)");
        }
        
    }

}