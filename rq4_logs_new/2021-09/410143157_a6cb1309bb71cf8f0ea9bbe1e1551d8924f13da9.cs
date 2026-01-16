using Xunit;
using BinaryRepresentor;

namespace BinaryRepresentor
{
    public class GetBitsToChangeFromDataTest
    {
        [Fact]
        public void GetBitsToChange_WithLittleEndianTrue()
        {
            // Arrange
            bool[] sourceBits = new bool[16] { false, false, false, true, true, true, true, true, true, true, true, false, false, false, true, true }; // ob_1111_1000, ob_1100_0111

            bool isLittleEndiann = true;
            ushort bitOffset = 4;
            ushort valueSize = 4;

            // Act
            bool[] result = BitCalculator.GetBitsToChangeFromSourceBits(sourceBits, isLittleEndiann, bitOffset, valueSize);

            // Assert
            Assert.Equal(new bool[] { true, true, true, true }, result);
        }

        [Fact]
        public void GetBitsToChange_WithBigEndianTrue()
        {
            // Arrange
            bool[] sourceBits = new bool[16] { false, false, false, true, true, true, true, true, true, true, true, false, false, false, true, true }; // ob_1111_1000, ob_1100_0111

            bool isLittleEndiann = false;
            ushort bitOffset = 8;
            ushort valueSize = 8;

            // Act
            bool[] result = BitCalculator.GetBitsToChangeFromSourceBits(sourceBits, isLittleEndiann, bitOffset, valueSize);

            // Assert
            Assert.Equal(new bool[] { true, true, false, false, false, true, true, true }, result);
        }
    }
}