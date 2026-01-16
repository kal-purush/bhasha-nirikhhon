namespace RuleEngineTester.RuleEngine.Evaluators;

public class ConditionEvaluatorFactory<T>
{
    public IConditionEvaluator<T> CreateConditionEvaluator(Condition condition)
    {
        switch (condition.ConditionType)
        {
            case ConditionType.Null:
                return new NullCondition<T>(condition.Name);
            case ConditionType.NotNull:
                return new NotNullCondition<T>(condition.Name);
            case ConditionType.NotEmpty:
                return new NotEmptyCondition<T>(condition.Name);
            case ConditionType.None:
                return new NoneCondition<T>();
            // Add more cases for other condition types
            default:
                throw new NotSupportedException($"Condition type '{condition.ConditionType}' is not supported.");
        }
    }
}