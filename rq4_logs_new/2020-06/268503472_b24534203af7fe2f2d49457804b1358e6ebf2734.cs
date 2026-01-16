using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace MinMaxIntConcat.Tests
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void WhenInserting50And9And2And1_Concat_ShouldBe95021()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("50", comparer.Compare);
            list.InsertSorted("9", comparer.Compare);
            list.InsertSorted("2", comparer.Compare);
            list.InsertSorted("1", comparer.Compare);
            Assert.AreEqual("95021", list.ConcatElements());
        }

        [Test]
        public void WhenInserting50And9And2And1_ReverseConcat_ShouldBe12509()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("50", comparer.Compare);
            list.InsertSorted("9", comparer.Compare);
            list.InsertSorted("2", comparer.Compare);
            list.InsertSorted("1", comparer.Compare);
            list.Reverse();
            Assert.AreEqual("12509", list.ConcatElements());
        }

        [Test]
        public void WhenInserting33333331And3_Concat_ShouldBe333333331()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("33333331", comparer.Compare);
            list.InsertSorted("3", comparer.Compare);
            Assert.AreEqual("333333331", list.ConcatElements());
        }

        [Test]
        public void WhenInserting33333331And3_ReverseConcat_ShouldBe333333313()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("33333331", comparer.Compare);
            list.InsertSorted("3", comparer.Compare);
            list.Reverse();
            Assert.AreEqual("333333313", list.ConcatElements());
        }

        [Test]
        public void WhenInserting11And112And121_Concat_ShouldBe12111211()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("11", comparer.Compare);
            list.InsertSorted("112", comparer.Compare);
            list.InsertSorted("121", comparer.Compare);
            Assert.AreEqual("12111211", list.ConcatElements());
        }

        [Test]
        public void WhenInserting11And112And121_ReverseConcat_ShouldBe11112121()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("11", comparer.Compare);
            list.InsertSorted("112", comparer.Compare);
            list.InsertSorted("121", comparer.Compare);
            list.Reverse();
            Assert.AreEqual("11112121", list.ConcatElements());
        }

        [Test]
        public void WhenInserting554And55And553_Concat_ShouldBe55554553()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("554", comparer.Compare);
            list.InsertSorted("55", comparer.Compare);
            list.InsertSorted("553", comparer.Compare);
            Assert.AreEqual("55554553", list.ConcatElements());
        }

        [Test]
        public void WhenInserting554And55And553_ReverseConcat_ShouldBe55554553()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("554", comparer.Compare);
            list.InsertSorted("55", comparer.Compare);
            list.InsertSorted("553", comparer.Compare);
            list.Reverse();
            Assert.AreEqual("55355455", list.ConcatElements());
        }

        [Test]
        public void WhenInserting5And556And55_Concat_ShouldBe556555()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("5", comparer.Compare);
            list.InsertSorted("556", comparer.Compare);
            list.InsertSorted("55", comparer.Compare);
            Assert.AreEqual("556555", list.ConcatElements());
        }

        [Test]
        public void WhenInserting5And556And55_ReverseConcat_ShouldBe555556()
        {
            var list = new List<string>();
            var comparer = new StringComparer();
            list.InsertSorted("5", comparer.Compare);
            list.InsertSorted("556", comparer.Compare);
            list.InsertSorted("55", comparer.Compare);
            list.Reverse();
            Assert.AreEqual("555556", list.ConcatElements());
        }
    }
}