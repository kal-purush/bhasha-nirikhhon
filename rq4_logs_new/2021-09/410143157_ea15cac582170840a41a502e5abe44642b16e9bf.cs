using BinaryRepresentor;
using Xunit;


namespace BinaryRepresentorTests
{
    public class CalculateTest
    {
        [Fact]
        public void Calculate_WithDivisor0_ShouldReturnIllegalOperation()
        {
            // Arrange
            byte[] data = new byte[8];

            bool isLittleEndiann = true;
            ushort bitOffset = 0;
            ushort valueSize = 0;
            int preScalingOffset = 0;
            int postScalingOffset = 0;
            uint multiplier = 0;
            uint divider = 0;

            string result = BitCalculator.Calculate(data, isLittleEndiann, bitOffset, valueSize, preScalingOffset, postScalingOffset, multiplier, divider);

            Assert.Equal("Illegal Operation", result);
        }

        [Fact]
        public void Calculate_WithSumOfBitOffsetAndValueSizeGreaterThan8TimesDataLength_ShouldReturnIllegalOperation()
        {
            // Arrange
            byte[] data = new byte[8];

            bool isLittleEndiann = true;
            ushort bitOffset = 33;
            ushort valueSize = 32;
            int preScalingOffset = 0;
            int postScalingOffset = 0;
            uint multiplier = 1;
            uint divider = 1;

            string result = BitCalculator.Calculate(data, isLittleEndiann, bitOffset, valueSize, preScalingOffset, postScalingOffset, multiplier, divider);

            Assert.Equal("Illegal Operation", result);
        }


    }
}