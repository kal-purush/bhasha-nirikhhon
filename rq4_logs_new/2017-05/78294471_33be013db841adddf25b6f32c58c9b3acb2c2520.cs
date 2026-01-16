using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ghosen.Tests
{
    [TestClass]
    public class KvaserTests
    {
        /// <summary>
        /// An empty line will leave all fields default initialized
        /// </summary>
        [TestMethod]
        public void KvaserEmptyLineTest()
        {
            // arrange
            var testLine = "";

            // act
            var result = ghosen.Parsers.Kvaser.KvaserLine.Parse(testLine);

            // assert
            Assert.AreEqual(default(DateTime), result.Time);
            Assert.AreEqual(string.Empty, result.Interface);
            Assert.AreEqual(null, result.Message);
        }

        /// <summary>
        /// A partial line will leave unspecified fields default initialized
        /// </summary>
        [TestMethod]
        public void KvaserPartialLineTest()
        {
            // arrange
            var testLine = " 1827.826927  1         7E0    Rx  ";

            // act
            var result = ghosen.Parsers.Kvaser.KvaserLine.Parse(testLine);

            // assert
            Assert.AreEqual(TimeSpan.FromSeconds(1827.826927), result.Time.TimeOfDay);
            Assert.AreEqual(string.Empty, result.Interface);
            Assert.AreEqual(null, result.Message);
        }
    }
}