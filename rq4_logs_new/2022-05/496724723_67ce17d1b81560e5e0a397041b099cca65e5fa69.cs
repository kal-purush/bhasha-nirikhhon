using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using MsTestingWithRegularExpression;

namespace MsTesingWithRegex
{
    [TestClass]
    public class UnitTest1
    {
        [TestClass]
        public class UserValidation
        {
            [TestMethod]
            //Checking for multiple first name
            [DataRow("Praful", true)]
            [DataRow("pr", false)]
            [DataRow("praful", false)]
            [DataRow("Pr", false)]
            [DataRow("Pra", true)]
            public void GivenFirstNameValidation(string firstName, bool expected) // Testing for Firstname Validation
            {
                //Arrange
                Validation validation = new Validation();
                //Act
                bool actual = validation.FirstNameValidation(firstName);
                //Assert
                Assert.AreEqual(expected, actual);
            }
        }
    }
}