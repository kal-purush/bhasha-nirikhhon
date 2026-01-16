using System.Linq.Expressions;

namespace RuleEngineTester.RuleEngine.Evaluators;

public class CompositeConditionEvaluator<T> : IConditionEvaluator<T>
{
    private readonly List<Condition> subConditions;

    public CompositeConditionEvaluator(List<Condition> subConditions)
    {
        this.subConditions = subConditions;
    }

    public bool Evaluate(T target)
    {
        // Default to true for an empty list of conditions
        if (subConditions == null || subConditions.Count == 0)
        {
            return true;
        }

        // Use AND logic for now; you might extend it to handle OR and other logical operators
        foreach (var condition in subConditions)
        {
            var conditionEvaluator = new ConditionEvaluatorFactory<T>().CreateConditionEvaluator(condition);
            if (!conditionEvaluator.Evaluate(target))
            {
                return false; // Short-circuit on the first false condition
            }
        }

        return true;
    }

    public Expression<Func<T, bool>> BuildExpression(ParameterExpression parameter)
    {
        // Build expressions for sub-conditions and combine them based on the logical operator
        Expression combinedExpression = null;

        foreach (var condition in subConditions)
        {
            var conditionEvaluator = new ConditionEvaluatorFactory<T>().CreateConditionEvaluator(condition);
            var conditionExpression = conditionEvaluator.BuildExpression(parameter);

            if (combinedExpression == null)
            {
                // First condition
                combinedExpression = conditionExpression?.Body;
            }
            else
            {
                if (conditionExpression != null)
                {
                    // Combine with AND or OR logic based on the condition's logical operator
                    combinedExpression = condition.Operator == OperatorType.And
                        ? Expression.AndAlso(combinedExpression, conditionExpression.Body)
                        : Expression.OrElse(combinedExpression, conditionExpression.Body);
                }
            }
        }



        //return Expression.Lambda<Func<T, bool>>(flattenedExpression, parameter);

        return Expression.Lambda<Func<T, bool>>(combinedExpression, parameter);
    }

    public static Expression FlattenAndAlso(Expression expression)
    {
        if (expression is BinaryExpression binaryExpression && binaryExpression.NodeType == ExpressionType.AndAlso)
        {
            // Flatten the left and right sides
            var left = FlattenAndAlso(binaryExpression.Left);
            var right = FlattenAndAlso(binaryExpression.Right);

            // Combine the conditions using AndAlso
            return Expression.AndAlso(left, right);
        }

        return expression;
    }
}