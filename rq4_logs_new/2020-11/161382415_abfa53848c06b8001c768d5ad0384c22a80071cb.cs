using System;
using Xunit;
using SolarLunarName.SharedTypes.Exceptions;
using SolarLunarName.SharedTypes.Primitives;

namespace SolarLunarName.SharedTypes.Tests
{
    public class ValidLunarDay_CastingValidationExceptions
    {
        [Theory]
        [InlineData(32)]
        [InlineData(Int32.MaxValue)]
        public void DayToBig(int day){
            Assert.Throws<DayOutOfRangeException>(() => (ValidLunarDay)day);
        }
        [Theory]
        [InlineData(-1)]
        [InlineData(Int32.MinValue)]
        public void YearToSmall(int day){
            Assert.Throws<DayOutOfRangeException>(() => (ValidLunarDay)day);
        }
        [Theory]
        [InlineData(31)]
        [InlineData(24)]
        [InlineData(1)]
        public void YearOK(int day){
            var x = (ValidLunarDay)day;
        } 


    }
}