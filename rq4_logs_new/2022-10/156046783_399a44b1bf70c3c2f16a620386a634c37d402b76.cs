using EAXUnitHelpers.Comparison.Tests.Models;
using System;
using Xunit;
using Xunit.Sdk;

namespace EAXUnitHelpers.Comparison.Tests
{
    public class CustomObjectComparerTests
    {
        [Fact]
        public void CustomObjectComparerShouldResultInTrueWhenTheComparedObjectsAreEqualForPrimitiveTypesAtTheTopLevel()
        {
            // arrange
            var actual = new Person
            {
                Age = 21,
                ChangeInPocket = .99m,
                DateOfBirth = DateTime.Today.AddYears(-21),
                FirstName = "Fifi",
                LastName = "LeGrange"
            };
            var expected = new Person
            {
                Age = 21,
                ChangeInPocket = .99m,
                DateOfBirth = DateTime.Today.AddYears(-21),
                FirstName = "Fifi",
                LastName = "LeGrange"
            };
            var comparer = new CustomObjectComparer<Person>();

            // act
            var result = comparer.Equals(expected, actual);

            // assert
            Assert.True(result);
        }

        [Fact]
        public void CustomObjectComparerShouldThrowAnEqualExceptionWhenTheComparedObjectsAreNotEqualForPrimitiveTypesAtTheTopLevel()
        {
            // arrange
            var actual = new Person
            {
                Age = 21,
                ChangeInPocket = .99m,
                DateOfBirth = DateTime.Today.AddYears(-21),
                FirstName = "Fifi",
                LastName = "LeGrange"
            };
            var expected = new Person
            {
                Age = 49,
                ChangeInPocket = .99m,
                DateOfBirth = DateTime.Today.AddYears(-21),
                FirstName = "Fifi",
                LastName = "LeGrange"
            };
            var comparer = new CustomObjectComparer<Person>();

            // act/assert
            Assert.Throws<EqualException>(() => comparer.Equals(expected, actual));
        }

        [Fact]
        public void CustomObjectComparerShouldIncludeTheCorrectErrorMessageWhenTheComparedObjectsAreNotEqualForPrimitiveTypesAtTheTopLevel()
        {
            // arrange
            var actual = new Person
            {
                Age = 21,
                ChangeInPocket = .99m,
                DateOfBirth = DateTime.Today.AddYears(-21),
                FirstName = "Fifi",
                LastName = "LeGrange"
            };
            var expected = new Person
            {
                Age = 49,
                ChangeInPocket = .99m,
                DateOfBirth = DateTime.Today.AddYears(-21),
                FirstName = "Fifi",
                LastName = "LeGrange"
            };
            var comparer = new CustomObjectComparer<Person>();
            var expectedMessage = "Assert.Equal() Failure\r\nExpected: A value of \"49\" for property \"Age\"\r\nActual:   A value of \"21\" for property \"Age\"";

            // act
            var exception = Assert.Throws<EqualException>(() => comparer.Equals(expected, actual));

            // assert
            Assert.Equal(expectedMessage, exception.Message);
        }
    }
}