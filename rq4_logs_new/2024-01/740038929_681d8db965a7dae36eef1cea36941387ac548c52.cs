using Antlr4.Runtime.Misc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Schema;
using RuleEngineTester.RuleEngine.Parser.Common.Resolvers;
using RuleEngineTester.RuleEngine.WeaklyTyped;
using static Antlr4.Runtime.Atn.SemanticContext;
using static RulesParser;

namespace RuleEngineTester.RuleEngine.Parser.Antlr
{
    public class LsRuleVisitor : RulesBaseVisitor<object>
    {
        private readonly JSchema _schema;
        private readonly ILogger _logger;
        public LsRuleVisitor(JSchema schema, ILogger logger)
        {
            _schema = schema;
            _logger = logger;
        }
        public string? AppliesTo { get; private set; }
        public string? RuleName { get; private set; }
        private ConditionBuilder _active_conditionbuilder;
        private List<RuleEngineTester.RuleEngine.WeaklyTyped.RuleBuilder> _rules = [];

        public IEnumerable<WeaklyTyped.IRule> ParsedRules => _rules.Select(s => s.Build());

        private RuleBuilder _activeRuleBuilder => _rules.Last();

        public override object VisitRuleFile(RulesParser.RuleFileContext context)
        {
            _rules.Add(WeaklyTyped.Rule.CreateBuilder(_logger));
            // Your logic for when visiting the ruleFile
            return base.VisitRuleFile(context);
        }


        public override object VisitRuleName([NotNull] RulesParser.RuleNameContext context)
        {
            _activeRuleBuilder.WithName(context.STRING().GetText());
            // Your logic here
            RuleName = context.STRING().GetText().Trim('\"');
            RuleName = RuleName.Trim('\"');
            return base.VisitRuleName(context);
        }

        public override object VisitAppliesTo([NotNull] RulesParser.AppliesToContext context)
        {
            _activeRuleBuilder.ForType(context.STRING().GetText().Trim('\"'), _schema);

            AppliesTo = context.STRING().GetText();
            AppliesTo = AppliesTo.Trim('\"');
            return base.VisitAppliesTo(context);
        }

        public object VisitBasicCondition([NotNull] RulesParser.BasicConditionContext context)
        {
            RuleCondition? cnd;
            
            // Initialize a variable to hold the condition representation
            string condition = "";

            if (context.ID() != null && context.comparator() != null && context.value() != null)
            {
                // Handle the ID comparator value structure
                string leftOperand = context.ID().GetText();
                string comparator = context.comparator().GetText();
                var rightOperand = Resolvers.ResolveOperandType(_schema, leftOperand,  context.value().GetText());

                condition = $"{leftOperand} {comparator} {rightOperand}";
                _logger.LogInformation(condition);
             
                if (_active_conditionbuilder != null)
                {

                    _active_conditionbuilder.AndCondition(leftOperand, ConditionOperatorResolver.ResolveType(comparator), rightOperand);
                }

            }
            else if (context.ID() != null && context.unaryOperator() != null)
            {
                // Handle the ID unaryOperator structure
                string leftOperand = context.ID().GetText();
                string unaryOperator = context.unaryOperator().GetText();
                var rightOperand = Resolvers.ResolveOperandType(_schema, leftOperand, context.value().GetText());

                condition = $"{leftOperand} {unaryOperator}";

                _active_conditionbuilder.AndCondition(leftOperand, ConditionOperatorResolver.ResolveType(unaryOperator), rightOperand);
                _logger.LogInformation(condition);

            }
            
            return base.VisitBasicCondition(context);
        }

        public override object VisitConditionBlock([NotNull] RulesParser.ConditionBlockContext context)
        {
            
            var conditionBuilder = RuleCondition.CreateBuilder(_logger);
            foreach (var logicalExpression in context.logicalExpression())
            {
                _active_conditionbuilder = RuleCondition.CreateBuilder(_logger);
                ProcessLogicalExpression(logicalExpression, ref _active_conditionbuilder);
                _activeRuleBuilder.AddCondition(_active_conditionbuilder.Build());
            }

            return base.VisitConditionBlock(context);
        }

        private void ProcessLogicalExpression(RulesParser.LogicalExpressionContext context, ref ConditionBuilder builder)
        {
            foreach (var expression in context.expression())
            {
                var basicConditionContext = expression.basicCondition();
                if (basicConditionContext != null)
                {
                    var conditionResult = VisitBasicCondition(basicConditionContext);
                }
                var logicalExpression = expression.logicalExpression();
                if (logicalExpression != null)
                {
                    ProcessLogicalExpression(logicalExpression, ref builder);
                }

            }
        }


        public override object VisitAction(RulesParser.ActionContext context)
        {
            string variableName = context.ID().GetText().Trim('\"');
            var mathExpressionContext = context.mathExpression();
            string expressionAsString = context.mathExpression().GetText().Trim('\"');
            _activeRuleBuilder.AddAction(new RuleAction(variableName, expressionAsString));
            _logger.LogInformation(variableName, mathExpressionContext);

            //// Process the math expression
           // var expressionResult = VisitMathExpression(mathExpressionContext);

            //// Assuming you have a method to handle action logic
            //HandleAction(variableName, expressionResult);

            return base.VisitAction(context);
        }

        public override object VisitMathExpression(RulesParser.MathExpressionContext context)
        {
            try
            {
                // Example: Evaluate the math expression and get a result
                double result = EvaluateMathExpression(context);

                // Return a successful result
                return MathExpressionResult.Success(result);
            }
            catch (Exception ex)
            {
                // Handle any exceptions and return a failure result
                return MathExpressionResult.Failure(ex.Message);
            }
        }


        private double EvaluateMathExpression(RulesParser.MathExpressionContext context)
        {
            foreach(var term in context.term())
            {
                var a = term.actionText();
                var b = term.ID().GetText();
                var c = term.NUMBER();

                var e = term.mathExpression();
            }
            //switch (context.mathoperators) switch
            //{
            //    case '*': context.term() + context. break;
            //    case '/': break;
            //    case '-': break;
            //    case '+': break;

            //}
            
            // Implement the logic to evaluate the math expression here
            // This might involve visiting child nodes and performing calculations

            // Example: return a calculated value
            return 42; // Replace with actual calculation logic
        }
        private void HandleAction(string variableName, object expressionResult)
        {
            // Implement the logic to handle the action
            // This might involve setting a variable, executing a command, etc.

            // Example: _someContext.SetVariable(variableName, expressionResult);
        }

    }
}

public class MathExpressionResult
{
    // The numerical result of the math expression
    public double Value { get; private set; }

    // Indicates whether the calculation was successful
    public bool IsSuccessful { get; private set; }

    // Any error message, in case the calculation was not successful
    public string ErrorMessage { get; private set; }

    public MathExpressionResult(double value, bool isSuccessful, string errorMessage = "")
    {
        Value = value;
        IsSuccessful = isSuccessful;
        ErrorMessage = errorMessage;
    }

    // Static method for creating a successful result
    public static MathExpressionResult Success(double value)
    {
        return new MathExpressionResult(value, true);
    }

    // Static method for creating a failed result
    public static MathExpressionResult Failure(string errorMessage)
    {
        return new MathExpressionResult(0, false, errorMessage);
    }
}