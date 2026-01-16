using System;
using System.Linq;
using FluentAssertions;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace FibonacciSequence.Tests
{
    public class Tests
    {
        [TestCase(0, 1)]
        [TestCase(1, 2)]
        [TestCase(1, 3)]
        [TestCase(2, 4)]
        [TestCase(3, 5)]
        [TestCase(34, 10)]
        [TestCase(2178309, 33)]
        [TestCase(165580141, 42)]
        [TestCase(1836311903, 47)]
        public void XNumber_ShouldBe_NthElementOfSequence(int expectedNumber, int elementNumber)
        {
            // Arrange / Act
            var fibonacciSequence = new FibonacciSequence(elementNumber);
            
            // Assert
            Assert.AreEqual(expectedNumber, fibonacciSequence.Last());
        }

        [TestCase(3)]
        [TestCase(4)]
        [TestCase(5)]
        [TestCase(6)]
        [TestCase(7)]
        [TestCase(8)]
        [TestCase(10)]
        [TestCase(22)]
        [TestCase(33)]
        [TestCase(42)]
        [TestCase(47)]
        public void NthElement_ShouldBe_SumOfTwoPreviousElements(int elementNumber)
        {
            // Arrange / Act
            var fibonacciSequence = new FibonacciSequence(elementNumber);

            // Assert
            Assert.AreEqual(fibonacciSequence.Last(), fibonacciSequence.SkipLast(1).TakeLast(2).Sum());
        }
        [TestCase(0, 0)]
        [TestCase(1, 1)]
        [TestCase(2, 2)]
        [TestCase(3,3)]
        [TestCase(4,4)]
        [TestCase(5,5)]
        [TestCase(6,6)]
        [TestCase(7,7)]
        [TestCase(8,8)]
        [TestCase(10,10)]
        [TestCase(22,22)]
        [TestCase(33,33)]
        [TestCase(42,42)]
        [TestCase(47, 47)]
        public void FirstNElementsSequenceCount_ShouldBe_N(int expectedCount, int sequenceCount)
        {
            // Arrange / Act
            var fibonacciSequence = new FibonacciSequence(sequenceCount);

            // Assert
            Assert.AreEqual(expectedCount, fibonacciSequence.ToList().Count);

        }
        
        [TestCase(48)]
        [TestCase(49)]
        [TestCase(50)]
        [TestCase(51)]
        [TestCase(58)]
        public void NSizedFibonacciSequence_ShouldThrow_ArgumentException(int sequenceCount)
        {
            // Arrange
            Action a = () => new FibonacciSequence(sequenceCount);

            // Act / Assert
            a.Should().Throw<ArgumentException>();
        }

        [TestCase(-1)]
        [TestCase(-2)]
        [TestCase(-3)]
        [TestCase(-23)]
        [TestCase(-25)]
        [TestCase(-99)]
        public void NegativeNumberSizedFibonacciSequence_ShouldThrow_ArgumentException(int sequenceCount)
        {
            // Arrange
            Action a = () => new FibonacciSequence(sequenceCount);

            // Act / Assert
            a.Should().Throw<ArgumentException>();
        }
    }
}