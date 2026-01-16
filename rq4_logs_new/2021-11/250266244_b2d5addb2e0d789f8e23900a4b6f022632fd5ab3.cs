using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using HelpMyStreet.Utils.Extensions;

namespace HelpMyStreet.UnitTests
{
    public class IEnumberableExtensionsTests
    {
        [SetUp]
        public void SetUp()
        {

        }

        [Test]
        public void ChunkBy()
        {
            var sample = Enumerable.Range(1, 35);

            var chunks = sample.ChunkBy(10);

            Assert.AreEqual(4, chunks.Count());
            Assert.AreEqual(35, chunks.SelectMany(n => n).Count());
            Assert.AreEqual(35, chunks.SelectMany(n => n).Distinct().Count());
            Assert.AreEqual(10, chunks.First().Count());
            Assert.AreEqual(5, chunks.Last().Count());
        }
    }
}