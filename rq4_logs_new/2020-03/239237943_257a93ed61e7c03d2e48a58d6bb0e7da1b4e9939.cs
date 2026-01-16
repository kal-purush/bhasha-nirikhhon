using System;
using System.ComponentModel.DataAnnotations;
using Xunit;


namespace TDD.tests
{
    public class CardTests
    {
        [Fact]
        public void When_ToppingUpTheCard_Expect_TopUpValue()
        {
            //arrange
            var card = new Card();

            //act
            card.TopUp(10);
            
            //assert
            Assert.Equal(10, card.Total);
        }

        [Fact]
        public void When()
        {
            //arrange
            var card = new Card();
            var tapOnId = Guid.Parse("c17479ba-7c57-4e73-a65c-461e230cde9d");
            
            //act
            card.TapOn(tapOnId);
            
            //assert
            Assert.Equal(tapOnId,card.TapOnId);
            
        }

        [Fact]
        public void test2()
        {
            //arrange
            var card = new Card();
            var machineId = Guid.Parse("c17479ba-7c57-4e73-a65c-461e230cde9d");
            var tapOnTime = new DateTime(2020, 2, 20);
            var tapOn = new TapOn(machineId, tapOnTime);
            
            //act
            card.TapOn(tapOn);
            
            //assert
            Assert.Equal( tapOnTime, card._TapOn.Time );
        }
        
        
    }
    
}