namespace Meridian.MeaningfulToString.Tests
{
    using Meridian.MeaningfulToString.Tests.Model;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class MeaningfulBaseTests
    {
        [TestMethod]
        public void ToString_SomethingSomething()
        {
            // Arrange
            Employee joe = new Employee()
            {
                Id = 13298278,
                Title = "Managing Director",
                FirstName = "Joe",
                LastName = "Bloggs",
                Email = "joe.bloggs@somecorp.local",
                Manager = null,
            };

            Employee fred = new Employee()
            {
                Id = 13298279,
                Title = "Head of Sales",
                FirstName = "Fred",
                LastName = "Smith",
                Email = "fred.smith@somecorp.local",
                Manager = joe,
            };

            string result = null;

            // Act
            result = fred.ToString();

            // Assert
        }
    }
}