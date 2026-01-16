using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using FluentAssertions;

namespace GoF.CasinoCraps.Tests
{
    public class PassLineBetTests
    {
        private PassLineBet bet;

        [SetUp]
        public void Setup()
        {
            bet = new PassLineBet();
        }

        [Test]
        public void Status_WhenCreated_IsActive()
        {
            bet.Status.Should().Be(BetStatus.Active);
        }

        [Test]
        public void Status_NaturalRolledForRound_IsWon()
        {
            bet.RoundEnded(RoundResult.Natural);

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void Status_CrapsRolledForRound_IsLost()
        {
            bet.RoundEnded(RoundResult.Craps);

            bet.Status.Should().Be(BetStatus.Lost);
        }

        [Test]
        public void Status_SevenOutRolledForRound_IsLost()
        {
            bet.RoundEnded(RoundResult.SevenOut);

            bet.Status.Should().Be(BetStatus.Lost);
        }

        [Test]
        public void Status_PointHitForRound_IsWon()
        {
            bet.RoundEnded(RoundResult.PointHit);

            bet.Status.Should().Be(BetStatus.Won);
        }
    }
}