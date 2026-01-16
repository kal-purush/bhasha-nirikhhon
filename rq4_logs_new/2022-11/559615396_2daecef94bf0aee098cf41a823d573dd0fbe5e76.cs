internal class ExpressionHandlerException : Exception { }
internal class OperatorIsMissing : ExpressionHandlerException { }
internal class OperatorIsWrong : ExpressionHandlerException
{
    public OperatorIsWrong(string operatorStr)
    {
        Data.Add("operator", operatorStr);
    }
}
internal class ExpressionIsWrong : ExpressionHandlerException { }
internal class OperandIsNotNumber : ExpressionHandlerException
{
    public string operatorStr;    
    public OperandIsNotNumber(string str)
    {
        operatorStr = str;
    }
}
internal class BadLuckNumber : Exception { }