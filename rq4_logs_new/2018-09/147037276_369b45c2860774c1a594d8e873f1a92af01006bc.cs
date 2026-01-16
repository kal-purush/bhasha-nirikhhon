using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace LINQtoCSV.Tests
{
    public class PersonRegex : Person, IAssertable<PersonRegex> {
        [CsvColumn(RegexPattern = "(?i)(male|female|other)")]
        public string Gender { get; set; }

        public void AssertEqual(PersonRegex other) {
            Assert.AreEqual(other.Name, Name);
            Assert.AreEqual(other.LastName, LastName);
            Assert.AreEqual(other.Age, Age);
            Assert.AreEqual(other.Gender, Gender);
        }
    }
}