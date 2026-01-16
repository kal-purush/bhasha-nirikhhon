package com.sinangenc;

import com.sinangenc.antlr4generatedfiles.PslGrammarLexer;
import com.sinangenc.antlr4generatedfiles.PslGrammarParser;
import com.sinangenc.visitor.PslTreeVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class Interpreter {
    public static void main(String[] args) {
        String code = """
                    var a = 5;
                    if(a < 6){
                    print a + "3";
                    }
                    """;

        Interpreter.analyzeAndRun(code);
    }

    private static void analyzeAndRun(String code){
        try {
            PslGrammarLexer lexer = new PslGrammarLexer(CharStreams.fromString(code));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PslGrammarParser parser = new PslGrammarParser(tokens);

            ParseTree tree = parser.program();

            PslTreeVisitor treeVisitor = new PslTreeVisitor();
            treeVisitor.visit(tree);
        }catch (IllegalArgumentException |
                NullPointerException e){
            System.err.println(e.getMessage());
        }
    }
}