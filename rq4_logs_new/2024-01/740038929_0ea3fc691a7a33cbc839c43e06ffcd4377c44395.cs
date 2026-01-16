using System;
using System.Linq.Expressions;

namespace RuleEngineTester.RuleEngine.Evaluators
{
    public class EqualsCondition<T> : ConditionEvaluatorBase<T>
    {
        private readonly string propertyName;
        private readonly object expectedValue;

        public EqualsCondition(string propertyName, object expectedValue)
        {
            this.propertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
            this.expectedValue = expectedValue;
        }

        public override bool Evaluate(T typedTarget)
        {
            var propertyValue = GetPropertyValue(typedTarget, propertyName);

            // Assuming propertyValue and expectedValue are of numeric types
            if (propertyValue is IComparable comparablePropertyValue && expectedValue is IComparable comparableExpectedValue)
            {
                return comparablePropertyValue.CompareTo(comparableExpectedValue) == 0;
            }

            // Default to false if types are not suitable for comparison
            return false;
        }



        public override Expression<Func<T, bool>> BuildExpression(ParameterExpression parameter)
        {
            // Build expression for the GreaterThanOrEquals condition
            var propertyExpression = Expression.Property(parameter, propertyName);
            var expectedValueExpression = Expression.Constant(expectedValue);
            var greaterThanOrEqualsExpression = Expression.GreaterThanOrEqual(propertyExpression, expectedValueExpression);

            return Expression.Lambda<Func<T, bool>>(greaterThanOrEqualsExpression, parameter);
        }
    }
}