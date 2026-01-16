using NUnit.Framework;
using System;
using testing_app;
using testing_app.Models;

namespace NUnitTestProject1
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [TestCase(0, 0)]
        [TestCase(3, 100)]
        [TestCase(5, 200)]
        [TestCase(10, 300)]
        public void Test_CalculatePenaltyFee(int point, int expected)
        {
            // Arrange
            Driver driver = new Driver();
            driver.PenaltyPoints = point;
            // Act
            var actual = driver.CalculatePenaltyFee();
            // Assess
            Assert.AreEqual(expected, actual);
        }

        [TestCase(0, false)]
        [TestCase(18, true)]
        [TestCase(50, true)]
        public void Test_isValidAge(DateTime currentDate, DateTime DOB, bool expected)
        {
            // Arrange
            Driver driver = new Driver();
            // Act
            var actual = driver.GetAge(currentDate, DOB);
            // Assess
            Assert.AreEqual(expected, actual);
        }

        [TestCase(InsuranceType.Comprehensive, 0.04)]
        [TestCase(InsuranceType.ThirdParty, 0.025)]
        public void Test_CalculatePremium(InsuranceType insuranceType, decimal expected)
        {
            // Arrange
            Driver driver = new Driver();
            driver.InsuranceType = insuranceType;
            // Act
            var actual = driver.CalculatePremium();
            // Assess
            Assert.AreEqual(expected, actual);
        }

        /*
        [Test]
        [TestCase(100, 18, Gender.Male, 0, 3.3)]
        [TestCase(100, 26, Gender.Male, 0, 3)]
        [TestCase(100, 18, Gender.Female, 0, 3)]
        [TestCase(100, 18, Gender.Male, 1, 103.3)]
        [TestCase(100, 26, Gender.Male, 2, 103)]
        [TestCase(100, 18, Gender.Female, 5, 203)]
        public void Test_CalculateQuote(decimal vehicleValue, int age, Gender input, int points, decimal expected)
        {
            // Arrange
            Driver driver = new Driver();
            driver.Gender = input;
            driver.VehicleValue = vehicleValue;
            driver.Age = age;
            driver.PenaltyPoints = points;
            // Act
            var actual = driver.CalculateQuote();
            // Assess
            Assert.AreEqual(expected, actual);
        }
        */
    }
}