using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using OverwatchSynergy.Api;
using OverwatchSynergy.Api.Heroes;
using OverwatchSynergy.Api.ObjectiveTypes;

namespace OverwatchSynergy.Tests
{
    [TestFixture]
    public class ObjectiveWeightTests
    {
        [Test]
        public void overallscores_can_be_retrieved_with_weighted_objective()
        {
            var overallscores = Calculator.GetOverallScoresForAllHeroes(new Hero[1] , new Hero[1], new NeutralCapture());

            overallscores.Single(s => s.Hero is Lucio).Value.Should().Be(37.5);
        }

        [Test]
        public void objective_weight_affects_overallscores()
        {
            var attackScores = Calculator.GetOverallScoresForAllHeroes(new Hero[1], new Hero[1], new AttackCapture());

            attackScores.Single(s => s.Hero is Lucio).Value.Should().Be(25);

            var overallscores = Calculator.GetOverallScoresForAllHeroes(new Hero[1], new Hero[1], new NeutralCapture());

            overallscores.Single(s => s.Hero is Lucio).Value.Should().Be(37.5);
        }
    }
}