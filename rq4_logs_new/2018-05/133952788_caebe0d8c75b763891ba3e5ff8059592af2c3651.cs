namespace Meridian.MeaningfulToString.Tests
{
    using Meridian.MeaningfulToString.Tests.Model;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ObjectExtensionsTests
    {
        [TestMethod]
        public void MeaningfulToString_OnlyIncludeClassProperties_EnsureOutputIsCorrect()
        {
            // Arrange
            Employee joe = new Employee()
            {
                Id = 13298278,
                Title = "Managing Director",
                FirstName = "Joe",
                LastName = "Bloggs",
                Emails = new string[]
                {
                    "joe.bloggs@somecorp.local",
                    "joeb1281@hotmail.co.uk"
                },
                Manager = null,
            };

            SoftwareDeveloper phillip = new SoftwareDeveloper()
            {
                Id = 13298329,
                Title = "Senior Software Developer",
                FirstName = "Phillip",
                LastName = "Jeffries",
                Emails = new string[] {
                    "phillip.jeffries@somecorp.local"
                },
                Manager = joe,
                Speciality = SoftwareDeveloper.SpecialityOption.CSharp,
            };

            string expectedToStringValue =
                "SoftwareDeveloper (" +
                "Speciality = CSharp" +
                ")";
            string actualToStringValue = null;

            // Act
            actualToStringValue = phillip.MeaningfulToString(
                includeBaseProperties: false);

            // Assert
            Assert.AreEqual(expectedToStringValue, actualToStringValue);
        }
    }
}