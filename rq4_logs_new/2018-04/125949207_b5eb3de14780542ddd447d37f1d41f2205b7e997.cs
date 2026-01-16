using KAryTree;
using System;
using Xunit;
using PrintLevelOrder;

namespace PrintLevelOrderTest
{
    public class PrintLevelOrderTesting
    {
        /// <summary>
        /// Tests three test cases from PrintLevelOrderTestingData class data
        /// against expected strings
        /// </summary>
        /// <param name="tree">Tree constructed from PrintLevelOrderTestingData</param>
        /// <param name="expectedOutput">The expected string output for PrintLevelOrder
        /// for the specified tree</param>
        [Theory]
        [ClassData(typeof(PrintLevelOrderTestingData))]
        public void PrintLevelOrderTest(KAryTree<char> tree, string expectedOutput)
        {
            // Assert
            Assert.Equal(expectedOutput, Program.PrintLevelOrder(tree));
        }
    }
}