using System;
using System.Text;
using System.Collections.Generic;
using NUnit.Framework;
using CollectionOfHelpers.GeneralExtensions;

namespace CollectionOfHelpersTests
{
    /// <summary>
    /// Summary description for DictionaryExtensions
    /// </summary>
    [TestFixture]
    public class DictionaryExtensions
    {
        [Test]
        public void Dictionary_tryadd_keyvalueexists()
        {
            //Arrange
            var dic = new Dictionary<string, string>();
            dic.Add("Hi", "there");

            //act
            var actual = dic.TryAdd("Hi", "there");

            //assert
            Assert.IsFalse(actual);
            Assert.AreEqual(dic.Count, 1);
        }

        [Test]
        public void Dictionary_tryadd_keyexists()
        {
            //Arrange
            var dic = new Dictionary<string, string>();
            dic.Add("Hi", "there");

            //act
            var actual = dic.TryAdd("Hi", "you");

            //assert
            Assert.IsFalse(actual);
            Assert.AreEqual(dic.Count, 1);
        }

        [Test]
        public void Dictionary_tryadd_keydoesntexist()
        {
            //Arrange
            var dic = new Dictionary<string, string>();
            dic.Add("Hi", "there");

            //act
            var actual = dic.TryAdd("Hello", "there");

            //assert
            Assert.IsTrue(actual);
            Assert.AreEqual(dic.Count, 2);
        }

        [Test]
        public void Dictionary_tryadd_keyvaluedoesntexist()
        {
            //Arrange
            var dic = new Dictionary<string, string>();
            dic.Add("Hi", "there");

            //act
            var actual = dic.TryAdd("Hello", "you");

            //assert
            Assert.IsTrue(actual);
            Assert.AreEqual(dic.Count, 2);
        }
    }
}