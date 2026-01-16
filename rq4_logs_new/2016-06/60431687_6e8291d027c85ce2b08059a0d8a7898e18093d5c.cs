using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;
using OverwatchSynergy.Api;
using OverwatchSynergy.Api.Heroes;
using System.Linq;
using FluentAssertions;

namespace OverwatchSynergy.Tests
{
    [TestFixture]
    public class OverallScoreTests
    {
        [Test]
        public void overallscores_can_be_retrieved()
        {
            var overallscores = Calculator.GetOverallScoresForAllHeroes(new[] { new Reaper() }, new[] { new Torbjorn() }, 1);

            overallscores.Single(s => s.Hero is Winston).Value.Should().Be(50);
        }
    }
}