using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace MultiplyList.Tests
{
    public class MultiplyListTests
    {
        [Fact]
        public void IterativeFunction_GivenAListOfNumbers_ReturnsProductOfNumbers()
        {
            // arrange
            List<int> list = new List<int> { 1, 2, 3, 4 };
            int expectedValue = list.Aggregate(1, (product, elm) => product * elm);
            // act
            int result = App.Program.MultiplyListIter(list);
            // assert
            Assert.Equal(expectedValue, result);
        }
        [Fact]
        public void IterativeFunction_GivenAListOfNumbersWithNegativeValue_ReturnsProductOfNumbers()
        {
            // arrange
            List<int> list = new List<int> { -1, 2 };
            int expectedValue = list.Aggregate(1, (product, elm) => product * elm);
            // act
            int result = App.Program.MultiplyListIter(list);
            // assert
            Assert.Equal(expectedValue, result);
        }
        [Fact]
        public void IterativeFunction_GivenAListOfNumbersWithZero_ReturnsZero()
        {
            // arrange
            List<int> list = new List<int> { 1, 2, 3, 0 };
            int expectedValue = 0;
            // act
            int result = App.Program.MultiplyListIter(list);
            // assert
            Assert.Equal(expectedValue, result);
        }
    }
}