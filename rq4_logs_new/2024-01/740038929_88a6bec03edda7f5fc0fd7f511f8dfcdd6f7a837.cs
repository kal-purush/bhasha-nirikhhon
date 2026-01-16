using LsRuleEngine.Interfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Schema;


namespace LsRuleEngine.Builders;


public class RuleBuilder(string ruleName, ILogger logger)
{

    private readonly ILogger _logger = logger;
    private readonly LsRule _rule = new(ruleName, logger);

    public RuleBuilder ForType(string type, JSchema jsonSchema)
    {
        _rule.SetType(type, jsonSchema);
        _logger.LogDebug("Type: {type}", type);
        _logger.LogDebug("Schema: {schema}", jsonSchema.ToString());
        return this;
    }

    //public RuleBuilder WithName(string name)
    //{
    //    _rule.AddName(name);
    //    return this;
    //}

    public RuleBuilder AddCondition(RuleCondition ruleCondition)
    {
        _rule.AddCondition(ruleCondition);
        return this;
    }

    public RuleBuilder AddAction(RuleAction action)
    {
        _rule.AddAction(action);
        return this;
    }

    public IRule Build() => _rule;
}
