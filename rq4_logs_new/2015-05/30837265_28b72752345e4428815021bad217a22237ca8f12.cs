using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using GoF.CasinoCraps;

namespace GoF.CasinoCraps.Tests
{
    public class ComeBetTests
    {
        private Game game;

        [SetUp]
        public void Setup()
        {
            game = new Game();
        }

        [Test]
        public void Status_PointEstablishedPointHit_IsWon()
        {
            Bet bet = new ComeBet(10);

            game.PlaceBet(bet);

            game.RollDice(4, 4);
            game.RollDice(4, 4);

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void Status_PointEstablishedSevenOut_IsLost()
        {
            Bet bet = new ComeBet(10);

            game.PlaceBet(bet);

            game.RollDice(4, 4);
            game.RollDice(4, 3);

            bet.Status.Should().Be(BetStatus.Lost);
        }

        [Test]
        public void Status_NaturalRolled_IsWon()
        {
            Bet bet = new ComeBet(10);

            game.PlaceBet(bet);

            game.RollDice(5, 6);

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void Status_CrapsRolled_IsLost()
        {
            Bet bet = new ComeBet(10);

            game.PlaceBet(bet);

            game.RollDice(1, 1);

            bet.Status.Should().Be(BetStatus.Lost);
        }

        [Test]
        public void PayoutAmount_NaturalRolled_IsCorrectAmount()
        {
            Bet bet = new ComeBet(10);

            game.PlaceBet(bet);

            game.RollDice(5, 6);

            bet.PayoutAmount.Should().Be(20);
        }
    }
}