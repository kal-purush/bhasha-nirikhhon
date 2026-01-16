using Microsoft.Extensions.Logging;
using RuleEngineTester.RuleEngine.Conditions;

namespace RuleEngineTester.RuleEngine.WeaklyTyped;

public class ConditionBuilder
{
    private RuleCondition _condition;

    private readonly ILogger _logger;

    public ConditionBuilder(ILogger logger)
    {
        _logger = logger;
        _condition = new(_logger);
    }

    public ConditionBuilder InitCondition(string property, ConditionType condition, Type type, object? value)
    {
        RuleConditionDefinition ruleConditionDefinition = new(
            property,
            condition,
            type,
            value,
            OperatorType.And);
        _condition.Add(ruleConditionDefinition);
        return this;
    }
    public ConditionBuilder AndCondition(string property, ConditionType condition, Type type, object? value)
    {
        RuleConditionDefinition ruleConditionDefinition = new(
           property,
           condition,
           type,
           value,
           OperatorType.And);
        _condition.Add(ruleConditionDefinition);

        return this;
    }
    public ConditionBuilder OrCondition(string property, ConditionType condition, Type type, object? value)
    {
        RuleConditionDefinition ruleConditionDefinition = new(
           property,
           condition,
           type,
           value,
           OperatorType.Or);
        _condition.Add(ruleConditionDefinition);

        return this;
    }
    public RuleCondition Build()
    {
        return _condition;
    }
}

