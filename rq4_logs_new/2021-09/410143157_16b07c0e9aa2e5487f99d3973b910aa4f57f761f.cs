using Xunit;
using BinaryRepresentor;

namespace BinaryRepresentor
{
    public class GetBitsFromDataTest
    {
        [Fact]
        public void GetBitsFromData_WithLittleEndianTrue()
        {
            // Arrange
            byte[] data = new byte[2] { 0b_0100_0110, 0b_0000_1001 };
            bool isLittleEndiann = true;

            // Act
            bool[] result = BitCalculator.GetBitsFromData(data, isLittleEndiann);

            // Assert
            Assert.Equal(new bool[] { false, true, true, false, false, false, true, false, true, false, false, true, false, false, false, false }, result);
        }

        [Fact]
        public void GetBitsFromData_WithLittleEndianFalse()
        {
            // Arrange
            byte[] data = new byte[2] { 0b_0100_0110, 0b_0000_1001 };

            bool isLittleEndiann = false;

            bool[] result = BitCalculator.GetBitsFromData(data, isLittleEndiann);

            Assert.Equal(new bool[] { true, false, false, true, false, false, false, false, false, true, true, false, false, false, true, false }, result);
        }
    }
}