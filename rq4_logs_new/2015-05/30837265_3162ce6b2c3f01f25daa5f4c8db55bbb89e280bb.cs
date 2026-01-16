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
    public class FieldBetTests
    {
        private Game game;

        [SetUp]
        public void Setup()
        {
            game = new Game();
        }

        [Test]
        public void Status_FieldBetPlacedTwoRolled_IsWon()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(1, 1);

            Bet bet = game.CompletedBets.First();

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void PayoutAmount_FieldBetPlacedTwoRolled_ReturnsCorrectAmount()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(1, 1);

            Bet bet = game.CompletedBets.First();

            bet.PayoutAmount.Should().Be(3);
        }

        [Test]
        public void Status_FieldBetPlacedThreeRolled_IsWon()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(2, 1);

            Bet bet = game.CompletedBets.First();

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void PayoutAmount_FieldBetPlacedThreeRolled_ReturnsCorrectAmount()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(2, 1);

            Bet bet = game.CompletedBets.First();

            bet.PayoutAmount.Should().Be(2);
        }

        [Test]
        public void Status_FieldBetPlacedFourRolled_IsWon()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(2, 2);

            Bet bet = game.CompletedBets.First();

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void PayoutAmount_FieldBetPlacedFourRolled_ReturnsCorrectAmount()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(2, 2);

            Bet bet = game.CompletedBets.First();

            bet.PayoutAmount.Should().Be(2);
        }

        [Test]
        public void Status_FieldBetPlacedNineRolled_IsWon()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(4, 5);

            Bet bet = game.CompletedBets.First();

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void PayoutAmount_FieldBetPlacedNineRolled_ReturnsCorrectAmount()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(4, 5);

            Bet bet = game.CompletedBets.First();

            bet.PayoutAmount.Should().Be(2);
        }

        [Test]
        public void Status_FieldBetPlacedTenRolled_IsWon()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(5, 5);

            Bet bet = game.CompletedBets.First();

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void PayoutAmount_FieldBetPlacedTenRolled_ReturnsCorrectAmount()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(5, 5);

            Bet bet = game.CompletedBets.First();

            bet.PayoutAmount.Should().Be(2);
        }

        [Test]
        public void Status_FieldBetPlacedElevenRolled_IsWon()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(6, 5);

            Bet bet = game.CompletedBets.First();

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void PayoutAmount_FieldBetPlacedElevenRolled_ReturnsCorrectAmount()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(6, 5);

            Bet bet = game.CompletedBets.First();

            bet.PayoutAmount.Should().Be(2);
        }

        [Test]
        public void Status_FieldBetPlacedTwelveRolled_IsWon()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(6, 6);

            Bet bet = game.CompletedBets.First();

            bet.Status.Should().Be(BetStatus.Won);
        }

        [Test]
        public void PayoutAmount_FieldBetPlacedTwelveRolled_ReturnsCorrectAmount()
        {
            game.PlaceBet(new FieldBet(1));

            game.RollDice(6, 6);

            Bet bet = game.CompletedBets.First();

            bet.PayoutAmount.Should().Be(4);
        }
    }
}