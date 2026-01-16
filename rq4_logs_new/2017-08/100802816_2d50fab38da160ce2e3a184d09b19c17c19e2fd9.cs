using Calculator.Add.Service;
using Xunit;

namespace Calculator.Add.Test
{
    public class AddServiceUnittest
    {
        private readonly AddService _uut;

        public AddServiceUnittest()
        {
            _uut = new AddService();
        }

        [Theory]
        [InlineData(0, -1)]
        [InlineData(0, 0)]
        [InlineData(0, 1)]
        [InlineData(1, -1)]
        [InlineData(1, 0)]
        [InlineData(1, 1)]
        [InlineData(-1, -1)]
        [InlineData(-1, 0)]
        [InlineData(-1, 1)]
        public void Add_AddingNumbers_ReturnsSum(int a, int b)
        {
            // Arrange
            var expected = a + b;

            // Act
            var result = _uut.Add(a, b);

            // Assert
            Assert.Equal(expected, result);
        }
    }
}