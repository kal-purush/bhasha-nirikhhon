using BinaryTree;
using System;
using Xunit;
using static FindMaximumValueBinaryTree.Program;

namespace FindMaximumValueBinaryTreeTest
{
    public class FindMaxValueTest
    {
        // FindMaxValueTestData is providing four test cases
        [Theory]
        [ClassData(typeof(FindMaxValueTestData))]
        public void FindMaximumValueTest(Tree<int> tree, int expectedMax)
        {
            // Act
            int actualMax = FindMaximumValue(tree);

            // Assert
            Assert.Equal(expectedMax, actualMax);
        }
    }
}