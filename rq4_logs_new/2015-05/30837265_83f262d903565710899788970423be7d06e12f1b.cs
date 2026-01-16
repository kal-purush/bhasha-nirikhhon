using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using FluentAssertions;

namespace GoF.CasinoCraps.Tests
{
    public class DontPassBetTests
    {
        private DontPassBet bet;

        [SetUp]
        public void Setup()
        {
            bet = new DontPassBet(100);
        }

        [Test]
        public void Status_WhenCreated_IsActive()
        {
            bet.Status.Should().Be(BetStatus.Active);
        }

        [Test]
        public void Status_CrapsRolledForRound_IsWon()
        {
            bet.RoundEnded(new RoundEndedEventArgs(RoundResult.Craps, new Roll(1, 1)));

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void Status_NaturalRolledForRound_IsLost()
        {
            bet.RoundEnded(new RoundEndedEventArgs(RoundResult.Natural, new Roll(6, 1)));

            bet.Status.Should().Be(BetStatus.Lost);
        }

        [Test]
        public void Status_TwelveRolledForRound_IsPush()
        {
            bet.RoundEnded(new RoundEndedEventArgs(RoundResult.Craps, new Roll(6, 6)));

            bet.Status.Should().Be(BetStatus.Push);
        }

        [Test]
        public void Status_SevenOutRolledForRound_IsWon()
        {
            bet.RoundEnded(new RoundEndedEventArgs(RoundResult.SevenOut, new Roll(6, 1)));

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void Status_PointHitForRound_IsLost()
        {
            bet.RoundEnded(new RoundEndedEventArgs(RoundResult.PointHit, new Roll(6, 2)));

            bet.Status.Should().Be(BetStatus.Lost);
        }

        [Test]
        public void PayoutAmount_BetPushed_ReturnsCorrectAmount()
        {
            bet.RoundEnded(new RoundEndedEventArgs(RoundResult.Craps, new Roll(6, 6)));

            bet.PayoutAmount.Should().Be(100);
        }
    }
}