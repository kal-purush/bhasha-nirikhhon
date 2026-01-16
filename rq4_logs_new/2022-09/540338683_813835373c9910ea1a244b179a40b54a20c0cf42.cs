using Microsoft.VisualStudio.TestPlatform.TestHost;
using NumbersGame;

namespace NumbersGameTests
{
    public class GameTests
    {
        [Fact]
        public void CheckGuess_SameInput_ReturnTrue()
        {
            bool result = Game.CheckGuess(0, 0);
            Assert.True(result);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(1)]
        public void CheckGuess_LowerHigherInput_ReturnTrue(int value)
        {
            bool result = Game.CheckGuess(value, 0);
            Assert.False(result);
        }
    }
}