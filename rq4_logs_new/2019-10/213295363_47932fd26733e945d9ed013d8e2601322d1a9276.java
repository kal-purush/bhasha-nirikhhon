package app;

/**
 * Interpreter
 */

class RuntimeError extends RuntimeException {
    final Token token;

    RuntimeError(Token token, String message) {
        super(message);
        this.token = token;
    }
}

public class Interpreter implements Expr.Visitor<Object> {

    @Override
    public Object visitLiteralExpr(Expr.Literal expr) {
        return expr.value;
    }

    @Override
    public Object visitGroupingExpr(Expr.Grouping expr) {
        // 执行表达式
        return evaluate(expr.expression);
    }

    @Override
    public Object visitUnaryExpr(Expr.Unary expr) {
        Token operator = expr.operator;
        Object right = evaluate(expr.right);

        checkNumberOperands(operator, right);
        switch (operator.type) {
        case MINUS:
            return -(double) right;
        case BANG:
            return !isTruthy(right);
        default:
            // 不会执行到这里
            break;
        }
        // 不会执行到这里
        return null;
    }

    @Override
    public Object visitBinaryExpr(Expr.Binary expr) {
        Object left = evaluate(expr.left);
        Object right = evaluate(expr.right);

        switch (expr.operator.type) {
        case GREATER:
            checkNumberOperands(expr.operator, left, right);
            return (double) left > (double) right;
        case GREATER_EQUAL:
            checkNumberOperands(expr.operator, left, right);
            return (double) left >= (double) right;
        case LESS:
            checkNumberOperands(expr.operator, left, right);
            return (double) left < (double) right;
        case LESS_EQUAL:
            checkNumberOperands(expr.operator, left, right);
            return (double) left <= (double) right;
        case MINUS:
            checkNumberOperands(expr.operator, left, right);
            return (double) left - (double) right;
        case SLASH:
            checkNumberOperands(expr.operator, left, right);
            return (double) left / (double) right;
        case STAR:
            checkNumberOperands(expr.operator, left, right);
            return (double) left * (double) right;
        case BANG_EQUAL:
            return !isEqual(left, right);
        case EQUAL_EQUAL:
            return isEqual(left, right);
        case PLUS:
            if (left instanceof Double && right instanceof Double) {
                return (double) left + (double) right;
            }

            if (left instanceof String && right instanceof String) {
                return (String) left + (String) right;
            }
            throw new RuntimeError(expr.operator, "Operands must be two numbers or two strings.");
        default:
            break;
        }

        // Unreachable.

        return null;

    }

    private void checkNumberOperands(Token operator, Object left, Object right) {
        if (left instanceof Double && right instanceof Double)
            return;

        throw new RuntimeError(operator, "Operands must be numbers.");
    }

    private void checkNumberOperands(Token operator, Object operand) {
        if (operand instanceof Double)
            return;

        throw new RuntimeError(operator, "Operands must be numbers.");
    }

    private boolean isEqual(Object a, Object b) {
        /**
         * 当两者都为 null 的时候相等 调用 object equal 会调用子类重写的方法 equal
         */
        if (a == null && b == null) {
            return true;
        }

        if (a == null) {
            return false;
        }

        return a.equals(b);
    }

    // 执行表达式
    private Object evaluate(Expr expr) {
        return expr.accept(this);
    }

    // isTruthy
    private boolean isTruthy(Object obj) {
        /**
         * 什么时候是假：null、数字 0、语言本身记录真假 除此之外全为真
         */
        if (obj == null) {
            return false;
        }
        // 由于 obj 是数字是全部由 Double 代替
        if (obj instanceof Double) {
            double value = (double) obj;
            return (value != 0);
        }

        if (obj instanceof Boolean)
            return (boolean) obj;

        return true;
    }

    private String stringify(Object object) {
        if (object == null)
            return "nil";
        if (object instanceof Double) {
            String text = object.toString();
            if (text.endsWith(".0")) {
                text = text.substring(0, text.length() - 2);
            }
            return text;
        }

        return object.toString();
    }

    public void interpret(Expr expr) {
        try {
            // 得到一个抽象语法树，然后执行这个抽象语法树
            Object value = evaluate(expr);
            System.out.println(stringify(value));
        } catch (RuntimeError error) {
            // TODO: handle exception
            MyLox.runtimeError(error);
        }
    }
}