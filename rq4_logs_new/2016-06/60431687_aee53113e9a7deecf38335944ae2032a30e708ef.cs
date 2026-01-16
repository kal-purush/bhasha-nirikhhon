using System.Linq;
using FluentAssertions;
using NUnit.Framework;
using OverwatchSynergy.Api;
using OverwatchSynergy.Api.Heroes;

namespace OverwatchSynergy.Tests
{
    [TestFixture]
    public class WeaknessTests
    {
        [Test]
        public void synergies_can_be_retrieved()
        {
            var results = Calculator.GetHeroesThatAreWeakAgainst(new[] { new Mei() });

            results.Single(s => s.Hero is Genji).Value.Should().Be(100);
        }

        [Test]
        public void synergies_are_averaged_when_a_retreiving_synergies_for_multiple_teamMates()
        {
            var synergies = Calculator.GetHeroesThatAreWeakAgainst(new[] { new Mei(), (Hero)new Bastion() });

            synergies.Single(s => s.Hero is Genji).Value.Should().Be(50);
        }
    }
}