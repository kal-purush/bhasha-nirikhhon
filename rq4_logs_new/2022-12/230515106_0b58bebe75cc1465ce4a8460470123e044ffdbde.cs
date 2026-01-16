using OrionSample.Api.Calculator;

namespace OrionSample.Api.Tests
{
    public class CalculatorTestBase
    {
        protected List<CalculatorOperation> BuildCalculatorOperationsList(
            decimal operand,
            int numberOfOperations,
            CalculatorOperationType operationType)
        {
            var result = new List<CalculatorOperation>();

            while (numberOfOperations > 0)
            {
                var op = new CalculatorOperation
                {
                    Operand = operand,
                    OperationType = operationType,
                };

                result.Add(op);
                numberOfOperations--;
            }

            return result;
        }
    }
}