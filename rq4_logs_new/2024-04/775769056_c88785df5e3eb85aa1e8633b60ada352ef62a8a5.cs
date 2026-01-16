using LagDaemon.YAMUD.Model.Utilities;

namespace LagDaemon.YAMUD.GameRulesEngine.Rules
{
    public class FunctionRule : RuleBase
    {
        private Func<IGameContext, bool> _function;

        public FunctionRule(Func<IGameContext, bool> function)
        {
            _function = function;
        }

        public override bool IsSatisfied(IGameContext context)
        {
            return _function(context);
        }

        public new IRule AddRule(IRule rule)
        {
            throw new ApplicationException("Do not add rules to this rule, they would never be run");
        }
    }
}