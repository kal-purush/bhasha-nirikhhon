using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Exponents
{
    public class TestProgram
    {
        //Much of the validation for this program was within the
        //main entry point. Added redundant checking into methods
        //**Added to MAIN**
        //Validation for non-number entry line 28
        //Moved variables outside of loop line 21
        //Removed redundant check for postive whole number check line 74 

        [Theory]
        [InlineData(2, 4)]
        [InlineData(4, 16)]
        [InlineData(6, 36)]
        [InlineData(8, 64)]
        [InlineData(3, 9)]
        [InlineData(11, 121)]
        [InlineData(-2, 0)]
        [InlineData(.1, 0)]
        [InlineData(0, 0)]
        public void TestGetSquared(int input, int expected)
        {
            int actual = Program.getSquared(input);
            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData(2, 8)]
        [InlineData(4, 64)]
        [InlineData(11, 1331)]
        [InlineData(-2, 0)]
        [InlineData(.1, 0)]
        [InlineData(0, 0)]
        public void TestGetCubed(int input, int expected)
        {
            int actual = Program.getCubed(input);
            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData(1, true)]
        [InlineData(100, true)]
        [InlineData(324, true)]
        [InlineData(0, true)]
        [InlineData(-1, false)]
        [InlineData(-157, false)]
        [InlineData(0.1, true)] //While technically false, int will make this zero, thus true. 
        public void TestCheck(int input, bool expected)
        {
            bool actual = Program.Check(input);
            Assert.Equal(expected, actual);
        }


    }
}