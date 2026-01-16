using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using FluentAssertions;
using GoF.CasinoCraps;

namespace GoF.CasinoCraps.Tests
{
    public class BigEightBetTests
    {
        private Game game;

        [SetUp]
        public void Setup()
        {
            game = new Game();
        }

        [Test]
        public void Status_BigEightBetPlacedEightOrSevenNotRolled_IsActive()
        {
            Bet bet = new BigEightBet(4);

            game.PlaceBet(bet);

            game.RollDice(1, 1);
            game.RollDice(2, 2);
            game.RollDice(4, 5);

            bet.Status.Should().Be(BetStatus.Active);
        }

        [Test]
        public void Status_BigEightBetPlacedEightRolled_IsWon()
        {
            Bet bet = new BigEightBet(4);

            game.PlaceBet(bet);

            game.RollDice(1, 1);
            game.RollDice(2, 2);
            game.RollDice(4, 5);
            game.RollDice(3, 5);

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void Status_BigEightBetPlacedSevenRolled_IsLost()
        {
            Bet bet = new BigEightBet(4);

            game.PlaceBet(bet);

            game.RollDice(1, 1);
            game.RollDice(2, 2);
            game.RollDice(4, 5);
            game.RollDice(1, 6);

            bet.Status.Should().Be(BetStatus.Lost);
        }

        [Test]
        public void PayoutAmount_BigEightBetPlacedEightRolled_IsCorrectAmount()
        {
            Bet bet = new BigEightBet(4);

            game.PlaceBet(bet);

            game.RollDice(1, 1);
            game.RollDice(2, 2);
            game.RollDice(4, 5);
            game.RollDice(3, 5);

            bet.PayoutAmount.Should().Be(8);
        }
    }
}