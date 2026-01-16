using System;
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using toofz.NecroDancer.Saves;
using toofz.NecroDancer.Tests.Properties;
using toofz.TestsShared;

namespace toofz.NecroDancer.Tests.Saves
{
    class SaveDataReaderTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void StreamIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                Stream stream = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new SaveDataReader(stream);
                });
            }

            [TestMethod]
            public void StreamIsNotSeekable_ThrowsArgumentException()
            {
                // Arrange
                var mockStream = new Mock<Stream>();
                mockStream.SetupGet(s => s.CanSeek).Returns(false);
                var stream = mockStream.Object;

                // Act -> Assert
                Assert.ThrowsException<ArgumentException>(() =>
                {
                    new SaveDataReader(stream);
                });
            }

            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var stream = Stream.Null;

                // Act
                var reader = new SaveDataReader(stream);

                // Assert
                Assert.IsInstanceOfType(reader, typeof(SaveDataReader));
            }
        }

        [TestClass]
        public class ReadMethod
        {
            [TestMethod]
            public void ReadsSaveData()
            {
                // Arrange
                var saveDataFiles = new[]
                {
                    Resources.SaveData,
                    Resources.SaveData_76561197960481221,
                    Resources.SaveData_76561198074553183,
                    Resources.SaveData_76561198252120446,
                };

                foreach (var saveDataFile in saveDataFiles)
                {
                    var readStream = new MemoryStream(Encoding.UTF8.GetBytes(saveDataFile));
                    var reader = new SaveDataReader(readStream);

                    // Act
                    var saveData = reader.Read();

                    // Assert
                    var writeStream = new MemoryStream();
                    var writer = new SaveDataWriter(writeStream);
                    writer.Write(saveData);
                    writeStream.Position = 0;
                    var sr = new StreamReader(writeStream);
                    var actual = sr.ReadToEnd();
                    Assert.That.NormalizedAreEqual(saveDataFile, actual);
                }
            }
        }
    }
}