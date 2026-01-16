
namespace RuleEngineTester.RuleEngine.Parser;

public class ConditionOperatorResolver
{
    private static readonly Dictionary<string, string> OperatorMappings = new Dictionary<string, string>
    {
        { ">=", "GreaterThanOrEquals" },
        { "GreaterThanOrEquals", "GreaterThanOrEquals" },
        { "Greater Than Or Equals", "GreaterThanOrEquals" },
        { "<=", "LessThanOrEquals" },
        { "LessThanOrEquals", "LessThanOrEquals" },
        { "Less Than Or Equals", "LessThanOrEquals" },
        { "<", "LessThan" },
        { "LessThan", "LessThan" },
        { "Less Than", "LessThan" },
        { ">", "GreaterThan" },
        { "GreaterThan", "GreaterThan" },
        { "Greater Than", "GreaterThan" },
        { "==", "Equals" },
        { "Equals", "Equals" },
        { "!=", "NotEquals" },
        { "NotEquals", "NotEquals" },
        { "Not Equals", "NotEquals" },
        { "isNotNull", "NotNull" },
        { "is not Null", "NotNull" },
        { "is null", "Null" },
        { "isNull", "Null" },
        { "notEmpty", "NotEmpty" },
        { "not Empty", "NotEmpty" },
        { "Empty", "Empty" },
        { "Composite", "Composite" },
    };

    public static string GetPattern() => string.Join('|', OperatorMappings.Select(m => $"({m.Key})"));
    public static string ResolveOperator(string description)
    {
        return OperatorMappings.TryGetValue(description.Trim(), out var result) ? result : string.Empty;
    }
}