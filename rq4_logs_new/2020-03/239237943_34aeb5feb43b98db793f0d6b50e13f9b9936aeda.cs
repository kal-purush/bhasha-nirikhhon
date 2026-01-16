using System;
using Xunit;

namespace Watch.UnitTests
{
    public class StopwatchTests
    { 
        [Fact]
        public void Start_MockDateTimeProvider_MockDateTimeNow()
        {
            //Arrange
            var stopWatch = new Stopwatch(new MockDateTimeProvider());
            //Act
            stopWatch.Start();
            
            //Assert
            Assert.Equal(new DateTime(2020,02,02, 10, 0,0 ), stopWatch.StartTime);
        }

        [Fact]
        public void Stop_MockDateProvider_Duration()
        {
            //Arrange 
            var stopwatch = new Stopwatch(new MockDateTimeProvider());
            
            stopwatch.Start();

            //Act
            var actualDuration = stopwatch.Stop();
            
            //Assert 
            var expectedDuration = new DateTime(2020, 02, 02, 10, 0, 0) - new DateTime(2020, 02, 02, 10, 0, 0);
            
            Assert.Equal(expectedDuration, actualDuration);
        }

        [Fact]  //TODO: is this how I should test throwing exceptions 
        public void Start_StartTwiceInARow_InvalidOperationException()
        {
            //Arrange
            var stopwatch = new Stopwatch(new MockDateTimeProvider());
            stopwatch.Start();
            
            //Act
            var exception = Assert.Throws<InvalidOperationException>(() => stopwatch.Start());
            
            //Assert
            Assert.Equal("Cannot start stopwatch twice in a row", exception.Message);
        }

    }
}


// [Fact]
// public void Stop_MockDateProvider_MockDateTimeNow()
// {
//     //Arrange
//     var stopWatch = new Stopwatch(new MockDateTimeProvider());
//     
//     //Act
//     stopWatch.Stop();
//     
//     //Assert
//     Assert.Equal(new DateTime(2020,02,02, 10, 0,0 ), stopWatch.StopTime);
// }


// Exercise 1: Design a StopwatchDesign a class called Stopwatch. The job of this class is to simulate a stopwatch.
// It should provide two methods: Start and Stop. We call the start method first, and the stop method next. 
// Then we ask the stopwatch about the duration between start and stop. Duration should be a value in TimeSpan. 
// Display the duration on the console. We should also be able to use a stopwatch multiple times. 
// So we may start and stop it and then start and stop it again. Make sure the duration value each time is calculated properly. 
// We should not be able to start a stopwatch twice in a row (because that may overwrite the initial start time). 
// So the class should throw an InvalidOperationException if its started twice. 1
// Educational tip: The aim of this exercise is to make you understand that a class should be always in a valid state. 
// We use encapsulation and information hiding to achieve that. The class should not reveal its implementation detail. 
// It only reveals a little bit, like a blackbox. From the outside, you should not be able to misuse a class because you shouldn’t 
// be able to see the implementation detail.