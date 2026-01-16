using System;
using Xunit;
using System.Collections.Generic;

namespace Assignment03.Tests
{
  public class DivisibleBySevenTest
  {
    [Theory]
    [InlineData(new object[] {new int[] {294, 70, 77, 2, 121, 9, 420, 7, 69, 0}, new int[]{294, 70, 77, 420}})] // chaos
    [InlineData(new object[] {new int[] {0, 7, 14, 21, 28, 35, 42}, new int[]{}})]                              // divisble by seven, but <= 42
    [InlineData(new object[] {new int[] {43, 44, 45, 46, 47, 48}, new int[]{}})]                                // > 42, but  not divisible 
    [InlineData(new object[] {new int[] {}, new int[]{}})]                                                      // empty list
    public void ReturnNumbersDivisibleBySevenAndGreaterThanFourtyTwo(params object[] data)
    {
      // Arrange
      IEnumerable<int> input = (IEnumerable<int>)data[0];

      IEnumerable<int> expected = (IEnumerable<int>) data[1];

      // Act
      IEnumerable<int> actual = input.DivisibleBySeven();

      // Assert
      Assert.Equal(expected, actual);
    }


  }
}