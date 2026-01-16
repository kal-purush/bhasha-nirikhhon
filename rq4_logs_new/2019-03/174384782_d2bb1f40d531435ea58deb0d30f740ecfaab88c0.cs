using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using TunesAPI;

namespace TunesUnitTests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestId_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Id = 1;

            //Assert
            Assert.AreEqual(1, t1.Id);
        }

        [TestMethod]
        public void TestTitle_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Title = "Hello";

            //Assert
            Assert.AreEqual("Hello", t1.Title);
        }

        [TestMethod]
        public void TestIrishChart_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.IrishChart = 1;

            //Assert
            Assert.AreEqual(1, t1.IrishChart);
        }

        [TestMethod]
        public void TestPrice_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Price = 17.99;

            //Assert
            Assert.AreEqual(17.99, t1.Price);
        }

        [TestMethod]
        public void TestArtist_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Artist = "Adele";

            //Assert
            Assert.AreEqual("Adele", t1.Artist);
        }

        [TestMethod]
        public void TestAlbum_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Album = "25";

            //Assert
            Assert.AreEqual("25", t1.Album);
        }

        [TestMethod]
        public void TestGenre_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Genre = "Pop";

            //Assert
            Assert.AreEqual("Pop", t1.Genre);
        }

        [TestMethod]
        public void TestDuration_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Duration = "3.29";

            //Assert
            Assert.AreEqual("3.29", t1.Duration);
        }

        [TestMethod]
        public void TestRelease_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.Realsed = new DateTime(2015,11,20);

            //Assert
            Assert.AreEqual(new DateTime(2015, 11, 20), t1.Realsed);
        }

        [TestMethod]
        public void TestBuyLink_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.BuyLink = "https://www.amazon.com/25-Adele/dp/B016WW10SA/ref=sr_1_1?crid=2Q85AFS5W1ET7&keywords=adele+25+cd&qid=1553430773&s=gateway&sprefix=25+cd+ad%2Caps%2C260&sr=8-1";

            //Assert
            Assert.AreEqual("https://www.amazon.com/25-Adele/dp/B016WW10SA/ref=sr_1_1?crid=2Q85AFS5W1ET7&keywords=adele+25+cd&qid=1553430773&s=gateway&sprefix=25+cd+ad%2Caps%2C260&sr=8-1", t1.BuyLink);
        }

        [TestMethod]
        public void TestAlbumCover_GetAndSet()
        {
            //Arrange
            Tunes t1 = new Tunes();

            //Act
            t1.AlbumCoverLink = "https://upload.wikimedia.org/wikipedia/en/thumb/9/96/Adele_-_25_%28Official_Album_Cover%29.png/220px-Adele_-_25_%28Official_Album_Cover%29.png";

            //Assert
            Assert.AreEqual("https://upload.wikimedia.org/wikipedia/en/thumb/9/96/Adele_-_25_%28Official_Album_Cover%29.png/220px-Adele_-_25_%28Official_Album_Cover%29.png", t1.AlbumCoverLink);
        }

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

        /*[TestMethod]
        public void TestTunesDBContext_GetAndSet()
        {
            //Arrange
            TunesContext tc1 = new TunesContext;

            //Act
            st1.Genre = "Pop";

            //Assert
            Assert.AreEqual("Pop", st1.Genre);
        }*/
    }
}