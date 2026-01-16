using Microsoft.VisualStudio.TestTools.UnitTesting;
using TunesAPI;

namespace SuggestedTunesUnitTests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestSTId_GetAndSet()
        {
            //Arrange
            SuggestedTunes st1 = new SuggestedTunes();

            //Act
            st1.Id = 1;

            //Assert
            Assert.AreEqual(1, st1.Id);
        }

        [TestMethod]
        public void TestSTTitle_GetAndSet()
        {
            //Arrange
            SuggestedTunes st1 = new SuggestedTunes();

            //Act
            st1.Title = "Hello";

            //Assert
            Assert.AreEqual("Hello", st1.Title);
        }

        [TestMethod]
        public void TestSTCount_GetAndSet()
        {
            //Arrange
            SuggestedTunes st1 = new SuggestedTunes();

            //Act
            st1.Count = 1;

            //Assert
            Assert.AreEqual(1, st1.Count);
        }

        [TestMethod]
        public void TestSTArtist_GetAndSet()
        {
            //Arrange
            SuggestedTunes st1 = new SuggestedTunes();

            //Act
            st1.Artist = "Adele";

            //Assert
            Assert.AreEqual("Adele", st1.Artist);
        }

        [TestMethod]
        public void TestSTGenre_GetAndSet()
        {
            //Arrange
            SuggestedTunes st1 = new SuggestedTunes();

            //Act
            st1.Genre = "Pop";

            //Assert
            Assert.AreEqual("Pop", st1.Genre);
        }
    }
}