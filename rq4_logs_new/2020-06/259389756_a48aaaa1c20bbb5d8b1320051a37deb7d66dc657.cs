using Hippologamus.Server.Validation;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Xunit;

namespace Hippologamus.Server.Test.Validation
{
    public class MustMatchTest
    {
        private class TestClass
        {
            public string ValueOne { get; set; }

            [MustMatch("ValueOne")]
            public string ValueTwo { get; set; }
        }

        private class DisplayNameTestClass
        {
            [DisplayName("Value One")]
            public string ValueOne { get; set; }

            [DisplayName("Value Two")]
            [MustMatch("ValueOne")]
            public string ValueTwo { get; set; }
        }

        [Theory]
        [InlineData("AA", "AA")]
        [InlineData("BB", "BB")]
        [InlineData("CC", "CC")]
        public void MustMatch_ValidCases(string valueOne, string valueTwo)
        {
            //Arrange
            var testClass = new TestClass();
            testClass.ValueOne = valueOne;
            testClass.ValueTwo = valueTwo;

            //Act
            var validationResults = new List<ValidationResult>();
            var ctx = new ValidationContext(testClass, null, null);
            Validator.TryValidateObject(testClass, ctx, validationResults, true);

            //Assert
            Assert.Empty(validationResults);
        }

        [Theory]
        [InlineData("AA", "ZZ")]
        [InlineData("BB", "ZZ")]
        [InlineData("CC", "ZZ")]
        public void MustMatch_InvalidCases(string valueOne, string valueTwo)
        {
            //Arrange
            var testClass = new TestClass();
            testClass.ValueOne = valueOne;
            testClass.ValueTwo = valueTwo;

            //Act
            var validationResults = new List<ValidationResult>();
            var ctx = new ValidationContext(testClass, null, null);
            Validator.TryValidateObject(testClass, ctx, validationResults, true);

            //Assert
            Assert.Equal(1, validationResults.Count());
        }

        [Theory]
        [InlineData("AA", "ZZ")]
        [InlineData("BB", "ZZ")]
        [InlineData("CC", "ZZ")]
        public void MustMatch_InvalidCases_TestClass_Message(string valueOne, string valueTwo)
        {
            //Arrange
            var testClass = new TestClass();
            testClass.ValueOne = valueOne;
            testClass.ValueTwo = valueTwo;

            //Act
            var validationResults = new List<ValidationResult>();
            var ctx = new ValidationContext(testClass, null, null);
            Validator.TryValidateObject(testClass, ctx, validationResults, true);

            //Assert
            Assert.Equal("ValueTwo must match ValueOne.", validationResults[0].ErrorMessage);
        }

        [Theory]
        [InlineData("AA", "ZZ")]
        [InlineData("BB", "ZZ")]
        [InlineData("CC", "ZZ")]
        public void MustMatch_InvalidCases_DisplayNameTestClass_Message(string valueOne, string valueTwo)
        {
            //Arrange
            var testClass = new DisplayNameTestClass();
            testClass.ValueOne = valueOne;
            testClass.ValueTwo = valueTwo;

            //Act
            var validationResults = new List<ValidationResult>();
            var ctx = new ValidationContext(testClass, null, null);
            Validator.TryValidateObject(testClass, ctx, validationResults, true);

            //Assert
            Assert.Equal("Value Two must match Value One.", validationResults[0].ErrorMessage);
        }
    }
}