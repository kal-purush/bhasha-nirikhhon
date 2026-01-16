using LagDaemon.YAMUD.GameRulesEngine.Rules;
using NUnit.Framework.Constraints;
using NRules.Fluent.Dsl;
using LagDaemon.YAMUD.Model.Utilities;
using NRules.RuleModel;
using NRules.Fluent;
using NRules;

namespace LagDaemon.YAMUD.GameRulesEngine.Tests
{
    public class NRulesRule : Rule
    {
        public override void Define()
        {
            GameContext context = null;

            // Define conditions
            When()
                .Match(() => context, c => c != null);

            // Define actions
            Then()
                .Do(ctx => ExecuteAction(context));
        }

        // Define the action to be executed when the rule fires
        private void ExecuteAction(GameContext context)
        {
            context.BooleanResult = true;
        }
    }



    public class BasicRuleTests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void NRulesIntegrationRule_returns_true_when_nrules_engine_sets_context_BooleanResult_to_true()
        {
            var ruleRepository = new RuleRepository();
            ruleRepository.Load(x => x.From(typeof(NRulesRule))); // Load your rule class here
            var factory = ruleRepository.Compile();

            NRulesIntegrationRule rule = new NRulesIntegrationRule(factory);

            Assert.IsTrue(rule.IsSatisfied(new GameContext() { CurrentUser = new Model.User.UserAccount() } ));
        }

        [Test]
        public void FunctionRule_returns_true_when_function_returns_true()
        {
            FunctionRule rule = new FunctionRule((context) => true);
            Assert.IsTrue(rule.IsSatisfied(null));
        }

        [Test]
        public void FunctionRule_returns_false_when_function_returns_false()
        {
            FunctionRule rule = new FunctionRule((context) => false);
            Assert.IsFalse(rule.IsSatisfied(null));
        }

        [Test]
        public void AndRule_returns_true_iff_all_subrules_return_true()
        {
            AndRule rule = new AndRule();
            rule.AddRule(new TrueRule());
            rule.AddRule(new TrueRule());
            rule.AddRule(new TrueRule());

            Assert.IsTrue(rule.IsSatisfied(null));
        }

        [Test]
        public void AndRule_returns_false_if_any_subrules_return_false()
        {
            AndRule rule = new AndRule();
            rule.AddRule(new TrueRule());
            rule.AddRule(new TrueRule());
            rule.AddRule(new FalseRule());

            Assert.IsFalse(rule.IsSatisfied(null));
        }

        [Test]
        public void NotRule_returns_true_if_subrule_return_false()
        {
            NotRule rule = new NotRule(new TrueRule());
            Assert.IsFalse(rule.IsSatisfied(null));
        }

        [Test]
        public void NotRule_returns_false_if_subrule_return_true()
        {
            NotRule rule = new NotRule(new FalseRule());
            Assert.IsTrue(rule.IsSatisfied(null));
        }

        [Test]
        public void OrRule_returns_true_if_any_subrule_returns_true()
        {
            OrRule rule = new OrRule();
            rule.AddRule(new FalseRule());
            rule.AddRule(new FalseRule());
            rule.AddRule(new TrueRule());

            Assert.IsTrue(rule.IsSatisfied(null));
        }

        [Test]
        public void OrRule_returns_false_if_all_subrules_return_false()
        {
            OrRule rule = new OrRule();
            rule.AddRule(new FalseRule());
            rule.AddRule(new FalseRule());
            rule.AddRule(new FalseRule());

            Assert.IsFalse(rule.IsSatisfied(null));
        }

        [Test]
        public void TrueRule_always_returns_true()
        {
            TrueRule rule = new TrueRule();
            Assert.IsTrue(rule.IsSatisfied(null));
        }

        [Test]
        public void FalseRule_always_returns_false()
        {
            FalseRule rule = new FalseRule();
            Assert.IsFalse(rule.IsSatisfied(null));
        }

        [Test]
        public void NotRule_does_not_allow_adding_subrules()
        {
            NotRule rule = new NotRule(new TrueRule());
            Assert.Throws<ApplicationException>(() => rule.AddRule(new TrueRule()));
        }
    }
}