using System;
using Xunit;
using StackAndQueue;
using TowersOfHanoi;
using System.Linq;

namespace TowersOfHanoiTest
{
    public class HanoiTest
    {
        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        [InlineData(8)]
        public void TowersOfHanoiTest(int n)
        {
            // Arrange
            // The ideal solution for Towers of Hanoi is (2^n)-1 moves. I am using a
            // technique inspired by https://stackoverflow.com/a/11880606 for raising
            // an integer to an exponent without rounding errors from System.Math.Pow()
            int idealMoves = Enumerable.Repeat(2, n).Aggregate(1, (a, b) => a * b) - 1;

            // Act
            MyQueue<string> moves = Program.TowersOfHanoi(n);

            // Assert
            Assert.Equal(moves.Length, idealMoves);
        }
    }
}