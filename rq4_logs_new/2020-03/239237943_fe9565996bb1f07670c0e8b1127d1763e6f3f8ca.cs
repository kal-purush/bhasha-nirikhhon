using System;
using Xunit;


namespace TDD.tests
{
    public class CardTests
    {
        [Fact]
        public void When_ToppingUpTheCard_Expect_TopUpValue()
        {
            //arrange
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);

            //act
            card.TopUp(10);
            
            //assert
            Assert.Equal(10, card.Total);
        }

        [Fact]
        public void When_TappingOn_Expect_CardToHaveMachineId()
        {
            //arrange
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);
            var machineId = Guid.Parse("c17479ba-7c57-4e73-a65c-461e230cde9d");
            var tapOnTime = new DateTime(2020, 01, 04);
            var tapOn = new Tap(machineId, tapOnTime );
            
            //act
            card.TapOn(tapOn);
            
            //assert
            Assert.Equal(machineId,card.TappedOn.MachineId);
            
        }

        [Fact]
        public void When_TappingOn_Expect_CardToHaveTapOnTime()
        {
            //arrange
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);
            var machineId = Guid.Parse("c17479ba-7c57-4e73-a65c-461e230cde9d");
            var tapOnTime = new DateTime(2020, 2, 20);
            var tapOn = new Tap(machineId, tapOnTime);
            
            //act
            card.TapOn(tapOn);
            
            //assert
            Assert.Equal( tapOnTime, card.TappedOn.Time );
        }

        [Fact]
        public void When_TappingOff_Expect_CardToHaveMachineId()
        {
            //arrange
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);
            var machineId = Guid.Parse("be2f9946-3bc3-47fc-ae3c-ab3bf261bbdf");
            var tapOffTime = new DateTime(2020, 2, 20,2,2,2);
            var tapOff = new Tap(machineId, tapOffTime);
 
            //act
            card.TapOff(tapOff);
            
            //assert
            Assert.Equal(Guid.Parse("be2f9946-3bc3-47fc-ae3c-ab3bf261bbdf"), card.TappedOff.MachineId);
        }

        [Fact]
        public void When_TappingOff_Expect_CardToHaveTapOffTime()
        {
            //arrange
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);
            var machineId = Guid.Parse("be2f9946-3bc3-47fc-ae3c-ab3bf261bbdf");
            var tapOffTime = new DateTime(2020, 2, 20,2,2,2);
            var tapOff = new Tap(machineId, tapOffTime);
            
            
            //act
            card.TapOff(tapOff);
            
            //assert
            Assert.Equal(tapOffTime, card.TappedOff.Time);
        }

        [Fact]
        public void Adjust()
        {
            //arrange
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);
            card.TopUp(10);
            var fare = 3m;
            
            //act
            card.Adjust(fare);
            
            //assert
            Assert.Equal(7, card.Total);
            
        }

        [Fact]
        public void When_TappingOff_Expect_CardToDeductFare()
        {
            //arrange 
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);
            card.TopUp(10);
            
            var machineId1 = Guid.Parse("c17479ba-7c57-4e73-a65c-461e230cde9d");
            var tapOnTime1 = new DateTime(2020, 01, 04);
            var tapOn = new Tap(machineId1, tapOnTime1);
            card.TapOn(tapOn);
            
            var machineId = Guid.Parse("be2f9946-3bc3-47fc-ae3c-ab3bf261bbdf");
            var tapOffTime = new DateTime(2020, 2, 20,2,2,2);
            var tapOff = new Tap(machineId, tapOffTime);
                
            //act
            card.TapOff(tapOff);
        
            //assert
            Assert.Equal(7, card.Total);
            
        }

        [Fact]
        public void When_AccessingTrips_Expect_ListOfAllTrips()
        {
            //arrange
            var fareCalculator = new FareCalculator();
            var card = new Card(fareCalculator);
            
            var machineId1 = Guid.Parse("c17479ba-7c57-4e73-a65c-461e230cde9d");
            var tapOnTime1 = new DateTime(2020, 01, 04);
            var tapOn = new Tap(machineId1, tapOnTime1);

            var machineId = Guid.Parse("be2f9946-3bc3-47fc-ae3c-ab3bf261bbdf");
            var tapOffTime = new DateTime(2020, 2, 20,2,2,2);
            var tapOff = new Tap(machineId, tapOffTime);
            
            //act
            card.TapOn(tapOn);
            card.TapOff(tapOff);

            //assert
            Assert.NotNull(card.Trips);
            Assert.NotEmpty(card.Trips);
        }

    }
    
}