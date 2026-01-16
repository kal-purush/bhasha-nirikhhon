using System;
using System.Collections.Generic;
using Xunit;

namespace FillRoomWithBoxes.Tests
{
    public class FillRoomWithBoxesTests
    {
        [Fact]
        public void GivenBoxesWhichCanFitInRoom_CalculateSolution()
        {
            // arrange
            List<int> possibleSizes = new List<int> { 7, 3, 1 };
            int roomSize = 25;
            List<int> boxes = new List<int>();
            List<int> expectedResult = new List<int> {7, 7, 7, 3, 1 };
            // act
            App.Program.FillRoomWithBoxes(roomSize, possibleSizes, ref boxes);
            // assert
            Assert.Equal(expectedResult, boxes);
        }
        [Fact]
        public void GivenABoxWhichCanFitInRoom_CalculateSolution()
        {
            // arrange
            List<int> possibleSizes = new List<int> { 7 };
            int roomSize = 14;
            List<int> boxes = new List<int>();
            List<int> expectedResult = new List<int> { 7, 7 };
            // act
            App.Program.FillRoomWithBoxes(roomSize, possibleSizes, ref boxes);
            // assert
            Assert.Equal(expectedResult, boxes);
        }
        [Fact]
        public void GivenBoxesWhichCannotFitInRoom_Throws()
        {
            // arrange
            List<int> possibleSizes = new List<int> { 3 };
            int roomSize = 4;
            List<int> boxes = new List<int>();
            string expectedException = "Unable to fit boxes into the room";
            // act
            // assert
            Exception ex = Assert.Throws<Exception>(() => App.Program.FillRoomWithBoxes(roomSize, possibleSizes, ref boxes));
            Assert.Equal(expectedException, ex.Message);
        }
    }
}