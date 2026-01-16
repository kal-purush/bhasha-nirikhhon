/*
 * Copyright (C) 2022 liraline
 * Copyright (C) 2022 luisfelipe3d
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package br.unicap.meow.compiler.controller;

import br.unicap.meow.compiler.model.Token;
import br.unicap.meow.compiler.model.TokenTypes;

public class SyntaticAnalyzer {
    private Compiler scanner;
    private Token token;

    public SyntaticAnalyzer(Compiler compiler) {
        this.scanner = compiler;
        this.token = compiler.getNextToken();
    }

    public void startingPoint_nonTerminal() {
        if (token != null && token.getLexeme().equals("int"))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t 'int' is missing");

        if (token != null && token.getLexeme().equals("main"))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t 'main' is missing");        

        if (token != null && token.getLexeme().equals("("))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t '(' is missing");
        
        if (token != null && token.getLexeme().equals(")"))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t ')' is missing");        

        blockOfCode_nonTerminal();
    }

    private void blockOfCode_nonTerminal() {
        if (token != null && token.getLexeme().equals("{"))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t '{' is missing");

        while (token != null && (token.getLexeme().equals("int") || token.getLexeme().equals("float") || token.getLexeme().equals("char"))){
            declaration_nonTerminal();
        }

        while (token != null && (token.getType().equals(TokenTypes.IDENTIFIER.typeCode) || token.getLexeme().equals("{")) || token.getLexeme().equals("while") || token.getLexeme().equals("if")) {
            command_nonTerminal();
        }

        if (token != null && token.getLexeme().equals("}"))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + "\t '}' is missing");
        
    }

    private void command_nonTerminal() {        
        if (token != null && token.getLexeme().equals("while"))
            iteration_terminal();
        else if (token != null && token.getLexeme().equals("if")) {
            token = scanner.getNextToken();

            if (token.getLexeme().equals("(")) {
                token = scanner.getNextToken();
            } else {
                throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + "\t '(' is missing");
            }

            relationalExoression_nonTerminal();

            if (token.getLexeme().equals(")")) {
                token = scanner.getNextToken();
            } else {
                throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + "\t ')' is missing");
            }

            command_nonTerminal();

            if (token.getLexeme().equals("else")) {
                token = scanner.getNextToken();
                command_nonTerminal();
            }                     
        } else
            basicCommand_nonTerminal();
    }

    private void basicCommand_nonTerminal() {
        if (token.getType().equals(TokenTypes.IDENTIFIER.typeCode))
            assignment_terminal();
        else
            blockOfCode_nonTerminal();
    }

    private void iteration_terminal() {
        if (token != null && token.getLexeme().equals("while"))
            token = scanner.getNextToken();
        else 
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() +
                "\t 'while' is missing"); 

        if (token != null && token.getLexeme().equals("("))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() +
                "\t '(' is missing");

        relationalExoression_nonTerminal();

        if (token != null && token.getLexeme().equals(")"))
            token = scanner.getNextToken();
        else 
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() +
            "\t ')' is missing");

        command_nonTerminal();
    }

    private void assignment_terminal() {
        if (token != null && token.getType().equals(TokenTypes.IDENTIFIER.typeCode))
            token = scanner.getNextToken();            
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() +
                "\t token should be an identifier");        

        if (token != null && token.getLexeme().equals("="))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() +
                "\t '=' is missing");

        arithmeticExpression_nonTerminal();

        if (token != null && token.getLexeme().equals(";"))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() +
                "\t ';' is missing");
    }

    private void relationalExoression_nonTerminal() {
        arithmeticExpression_nonTerminal();
        if (token != null && token.getType().equals(TokenTypes.RELATIONAL_OPERATOR.typeCode)) {
            token = scanner.getNextToken();
        }
        arithmeticExpression_nonTerminal();
    }

    private void arithmeticExpression_nonTerminal() {
        term_nonTerminal();
        arithmeticExpression_line_nonTerminal();
    }

    private void arithmeticExpression_line_nonTerminal() {
        if (token.getLexeme().equals("+") || token.getLexeme().equals("-")) {
            token = scanner.getNextToken();
            term_nonTerminal();
        }
    }

    private void term_nonTerminal() {
        factor_terminal();
        term_line_nonTerminal();
    }

    private void term_line_nonTerminal() {
        if (token.getLexeme().equals("*") || token.getLexeme().equals("/")) {
            token = scanner.getNextToken();
            factor_terminal();
        }
    }

    private void factor_terminal() {
        if (token != null && token.getLexeme().equals("(")) { 
            token = scanner.getNextToken();
            arithmeticExpression_nonTerminal();

            if (!token.getLexeme().equals(")")) {
                throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t ')' is missing");
            }            
        } else if (token != null && (token.getType().equals(TokenTypes.IDENTIFIER.typeCode) || token.getType().equals(TokenTypes.REAL.typeCode) ||
            token.getType().equals(TokenTypes.INTEGER.typeCode) || token.getType().equals(TokenTypes.CHAR.typeCode))) {
            token = scanner.getNextToken();
        }
    }

    private void declaration_nonTerminal() {
        if (token != null && (token.getLexeme().equals("int") || token.getLexeme().equals("float") || token.getLexeme().equals("char")))
                token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t token should be a type");
        
        if (token != null && token.getType().equals(TokenTypes.IDENTIFIER.typeCode))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t token should be an identifier");
        
        if (token != null && token.getLexeme().equals(";"))
            token = scanner.getNextToken();
        else
            throw new RuntimeException("ERROR on row " + scanner.getErrorRow() + " and column " + scanner.getErrorColumn() + " near " + token.getLexeme() + "\t ';' is missing");
        
    }
}