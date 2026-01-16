package Expressions;

import java.util.HashMap;
import Compiler.Token;

public class OperatorFactory {
    HashMap<String, BaseOperator> operators;

    public OperatorFactory() {
        this.operators = new HashMap<>();
        this.operators.put("*", new BaseOperator("*", false, 15));
        this.operators.put("/", new BaseOperator("/", false, 15));
        this.operators.put("+", new BaseOperator("+", false, 7));
        this.operators.put("-", new BaseOperator("-", false, 7));
        this.operators.put("<", new BaseOperator("<", false, 6));
        this.operators.put("<=", new BaseOperator("<=", false, 6));
        this.operators.put(">", new BaseOperator(">", false, 6));
        this.operators.put(">=", new BaseOperator(">=", false, 6));
        this.operators.put("==", new BaseOperator("==", false, 5));
        this.operators.put("~=", new BaseOperator("~=", false, 5));
        this.operators.put("and", new BaseOperator("and", false, 4));
        this.operators.put("or", new BaseOperator("or", false, 4));
        this.operators.put("=", new BaseOperator("=", true, 3));
    }


    public HashMap<String, BaseOperator> getOperators() {
        return this.operators;
    }

    public BaseOperator getOperator(Token token) {
        return this.operators.get(token.lexeme);
    }

}