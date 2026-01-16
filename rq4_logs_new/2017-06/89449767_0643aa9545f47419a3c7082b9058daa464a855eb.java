package Parsing;

import Expressions.ASTNode;
import Expressions.BaseOperator;
import Expressions.Node;
import Compiler.*;
import Expressions.ValueNode;

import java.util.Objects;

public class ASTParser {

    public static String parseAST(ASTNode tree) {
        Node value = tree.getValue();
        String sentence = "";

        if (value instanceof ValueNode) {
            return ((ValueNode) value).getValue();
        }
        if (value instanceof BaseOperator) {
            ASTNode leftNode = tree.getLeftASTNode();
            ASTNode rightNode = tree.getRightASTNode();
            String operationCode = "";

            switch (((BaseOperator) value).getSymbol()) {
                case "/":
                    operationCode = "DIV";
                    break;
                case "-":
                    operationCode = "SBI";
                    break;
                case "*":
                    operationCode = "MPI";
                    break;
                case "+":
                    operationCode = "ADD";
                    break;
                case "=":
                    operationCode = "LOD";
            }


            if (leftNode != null && rightNode != null) {
                if (Objects.equals(operationCode, "LOD")) {
                    Variable left = (Variable) leftNode.getValue();
                    return "LDA " + left.getName() + "\n" + parseAST(rightNode);

                }
                else if (leftNode.getValue() instanceof  ValueNode && rightNode.getValue() instanceof  ValueNode) {
                    ValueNode left = (ValueNode) leftNode.getValue();
                    sentence += "LDC " + left.getValue() + "\n";

                    ValueNode right = (ValueNode) rightNode.getValue();
                    sentence += "LCC " + right.getValue() + "\n";

                    sentence += operationCode + "\n";

                    return sentence;
                }
                else if (rightNode.getValue() instanceof BaseOperator && leftNode.getValue() instanceof  BaseOperator) {
                    return parseAST(leftNode) + parseAST(rightNode);
                }
                else if (leftNode.getValue() instanceof BaseOperator) {
                    ValueNode right = (ValueNode) rightNode.getValue();
                    sentence += parseAST(leftNode) + "LCC " + right.getValue() + "\n";
                    sentence += operationCode + "\n";
                    return sentence;
                } else if (rightNode.getValue() instanceof BaseOperator) {
                    ValueNode left = (ValueNode) leftNode.getValue();
                    sentence += parseAST(rightNode) + "LDC " + left.getValue() + "\n";
                    sentence += operationCode + "\n";
                    return sentence;
                }
            }


        }

        return sentence;
    }

}