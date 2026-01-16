using Xunit;
using System;

namespace TDD.tests
{
    public class FareCalculatorTests
    {
        [Fact]
        public void test()
        {
            //arrange
            var machineId = Guid.Parse("c17479ba-7c57-4e73-a65c-461e230cde9d");
            var tapOnTime = new DateTime(2020, 01, 04);
            var tapOn = new Tap(machineId, tapOnTime);
            
            var machineId2 = Guid.Parse("be2f9946-3bc3-47fc-ae3c-ab3bf261bbdf");
            var tapOffTime = new DateTime(2020, 2, 20,2,2,2);
            var tapOff = new Tap(machineId2, tapOffTime);
            
            var fareCalculator = new FareCalculator();
            
            //act
            var calculatedFare = fareCalculator.CalculateFare(tapOn, tapOff);
            
            //assert
            Assert.Equal(3, calculatedFare);
        }
    }
}