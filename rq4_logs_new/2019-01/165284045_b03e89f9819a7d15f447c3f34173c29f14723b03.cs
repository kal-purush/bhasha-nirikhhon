using System;
using Xunit;
using Lab04_TicTacToe.Classes;

namespace XUnitTest_Lab04_TicTacToe
{
    public class UnitTest1
    {
        [Fact]
        public void CanWin()
        {
            Player p1 = new Player();
            Player p2 = new Player();
            p1.Marker = "X";
            p2.Marker = "O";
            Game game = new Game(p1, p2);
            game.Board.GameBoard[0, 0] = "O";
            game.Board.GameBoard[1, 0] = "O";
            game.Board.GameBoard[2, 0] = "O";
            Assert.True(game.CheckForWinner(game.Board));
        }
    }
}