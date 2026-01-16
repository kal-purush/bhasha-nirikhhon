using System;
using ParseTreeNamespace;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ParserTests
{
    [TestClass]
    public class ParseTreeTest
    {
        ParseTree tree;

        [TestInitialize]
        public void Initialize()
        {
            tree = new ParseTree();
        }

        [TestMethod]
        public void Expression1Test()
        {
            tree.CreateTree("- * / 15 - 7 + 1 1 3 + 2 + 1 1");
            var result = tree.Calculate();
            Assert.IsTrue(result == 5);
        }

        [TestMethod]
        public void Expression2Test()
        {
            tree.CreateTree("- * / 15 - 7 2 3 + 2 + 1 1");
            var result = tree.Calculate();
            Assert.IsTrue(result == 5);
        }

        [TestMethod]
        public void Expression3Test()
        {
            tree.CreateTree("- * / 15 5 3 + 2 + 1 1");
            var result = tree.Calculate();
            Assert.IsTrue(result == 5);
        }

        [TestMethod()]
        [ExpectedException(typeof(MyException.MyDividedByZeroException))]
        public void DividedByZeroTest()
        {
            tree.CreateTree("/ 6 * 5 - 3 + 1 2");
            tree.Calculate();
        }

        [TestMethod()]
        [ExpectedException(typeof(MyException.InvalidExpressionException))]
        public void InvalidExpressionTest()
        {
            tree.CreateTree("+ 1 - 2");
        }
    }
}