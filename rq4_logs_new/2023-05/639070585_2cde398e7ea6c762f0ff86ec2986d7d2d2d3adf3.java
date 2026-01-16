public class FoldConstants implements Transformer{
    @Override
    public Expression transformNumber(Number num) {
        return new Number(num.getValue());
    }

    @Override
    public Expression transformBinaryOperation(BinaryOperation binOp) {
        Expression left = binOp.getLeft().transform(this);
        Expression right = binOp.getRight().transform(this);
        if (left instanceof Number && right instanceof Number) {
            double newValue = 0.0;
            double left_ = left.evaluate();
            double right_ = right.evaluate();
            switch (binOp.getOperation()) {
                case BinaryOperation.PLUS:
                    newValue = left_ + right_;
                    break;
                case BinaryOperation.MINUS:
                    newValue = left_ - right_;
                    break;
                case BinaryOperation.DIV:
                    newValue = left_ / right_;
                    break;
                case BinaryOperation.MUL:
                    newValue = left_ * right_;
                    break;
            }
            return new Number(newValue);
        }
        return new BinaryOperation(left, binOp.getOperation(), right);
    }

    @Override
    public Expression transformFunctionCall(FunctionCall funCall) {
        Expression arg = funCall.getArg().transform(this);
        if (arg instanceof Number) {
            double newValue;
            if (funCall.getName().equals("sqrt")) {
                newValue = Math.sqrt(arg.evaluate());
            } else {
                newValue = Math.abs(arg.evaluate());
            }
            return new Number(newValue);
        }
        return new FunctionCall(funCall.getName(), arg);
    }

    @Override
    public Expression transformVariable(Variable var) {
        return new Variable(var.getName());
    }
}