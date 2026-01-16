using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProgramNamespace;

namespace FoldTests
{
    [TestClass]
    public class FoldTest
    {
        List<int> list;

        [TestInitialize]
        public void Initialize()
        {
            list = new List<int>() { -4, -3, -2, -1, 0, 1, 2, 3, 4 };
        }

        [TestMethod]
        public void MultiplicationTest()
        {
            var result = Program.Fold(list, 0, (acc, elem) => acc * elem);
            Assert.IsTrue(result == 0);
        }

        [TestMethod]
        public void AdditionTest()
        {
            var result = Program.Fold(list, 5, (acc, elem) => acc + elem);
            Assert.IsTrue(result == 5);
        }
    }
}