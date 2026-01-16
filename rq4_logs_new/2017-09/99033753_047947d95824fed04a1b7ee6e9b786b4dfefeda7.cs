using System;
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using toofz.NecroDancer.Data;
using toofz.NecroDancer.Tests.Properties;
using toofz.TestsShared;

namespace toofz.NecroDancer.Tests.Data
{
    class NecroDancerDataSerializerTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange -> Act
                var serializer = new NecroDancerDataSerializer();

                // Assert
                Assert.IsInstanceOfType(serializer, typeof(NecroDancerDataSerializer));
            }
        }

        [TestClass]
        public class DeserializeMethod
        {
            [TestMethod]
            public void StreamIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var serializer = new NecroDancerDataSerializer();
                Stream stream = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    serializer.Deserialize(stream);
                });
            }

            [TestMethod]
            public void ReturnsNecroDancerData()
            {
                // Arrange
                var serializer = new NecroDancerDataSerializer();
                var stream = new MemoryStream(Encoding.UTF8.GetBytes(Resources.NecroDancerData));

                // Act
                var necroDancerData = serializer.Deserialize(stream);

                // Assert
                Assert.IsInstanceOfType(necroDancerData, typeof(NecroDancerData));
            }
        }

        [TestClass]
        public class SerializeMethod
        {
            [TestMethod]
            public void StreamIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var serializer = new NecroDancerDataSerializer();
                Stream stream = null;
                var necroDancerData = new NecroDancerData();

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    serializer.Serialize(stream, necroDancerData);
                });
            }

            [TestMethod]
            public void NecroDancerDataIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var serializer = new NecroDancerDataSerializer();
                var stream = Stream.Null;
                NecroDancerData necroDancerData = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    serializer.Serialize(stream, necroDancerData);
                });
            }

            [TestMethod]
            public void SerializesNecroDancerData()
            {
                // Arrange
                var serializer = new NecroDancerDataSerializer();
                var writeStream = new MemoryStream();
                var readStream = new MemoryStream(Encoding.UTF8.GetBytes(Resources.NecroDancerDataBaseline));
                var necroDancerData = serializer.Deserialize(readStream);

                // Act
                serializer.Serialize(writeStream, necroDancerData);

                // Assert
                var sr = new StreamReader(writeStream);
                writeStream.Position = 0;
                var actual = sr.ReadToEnd();
                Assert.That.NormalizedAreEqual(Resources.NecroDancerDataBaseline, actual);
            }
        }
    }
}