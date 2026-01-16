using System;
using Xunit;
using KAryTree;
using System.Linq;

namespace KAryTest
{
    public class KAryTreeTest
    {
        [Fact]
        public void CanConstructKAryTree()
        {
            // Arrange
            const int rootValue = 5;

            // Act
            KAryTree<int> tree = new KAryTree<int>(rootValue);

            // Assert
            Assert.Equal(rootValue, tree.Root.Value);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(10)]
        public void AddNodeAndGetCountTest(int addedNodeCount)
        {
            // Arrange
            KAryTree<int> tree = new KAryTree<int>(0);

            // Act
            // Keep adding nodes to the last added node (basically a linked list)
            for (int i = 0; i < addedNodeCount; i++)
            {
                tree.Add(i + 1, i);
            }

            // Assert
            // Total count should be addedNodeCount + 1 (to account for root)
            Assert.Equal(addedNodeCount + 1, tree.Count);
        }

        [Theory]
        [InlineData(5, new int[] { 10, 15 }, new int[] { 5, 5 }, new int[] { 5, 10, 15 })]
        [InlineData(10, new int[] { 15, 5, 20, 30, 25 }, new int[] { 10, 15, 15, 5, 30 }, new int[] { 10, 15, 5, 20, 30, 25 })]
        [InlineData(15, new int[] { }, new int[] { }, new int[] { 15 })]
        public void BreadthFirstTest(int rootValue,
            int[] addedValues, int[] addedParentValues, int[] expectedBFT)
        {
            // Verify theory inputs. Lengths of addedValues and addedParentValues should
            // match, and length of expectedBFT should match their lengths + 1
            if (addedValues.Length != addedParentValues.Length ||
                expectedBFT.Length != addedValues.Length + 1)
            {
                throw new ArgumentException();
            }

            // Arrange
            KAryTree<int> tree = new KAryTree<int>(rootValue);

            // Act
            for (int i = 0; i < addedValues.Length; i++)
            {
                tree.Add(addedValues[i], addedParentValues[i]);
            }

            // Assert
            Assert.Equal(expectedBFT, tree.BreadthFirstTraversal().ToArray());
        }

        [Fact]
        public void CanSearchForNode()
        {
            // Arrange
            KAryTree<int> tree = new KAryTree<int>(5);
            tree.Add(10, 5);
            tree.Add(15, 5);
            tree.Add(20, 5);
            tree.Add(25, 15);
            tree.Add(30, 25);

            // Act
            KAryNode<int> node = tree.Search(30);

            // Assert
            Assert.NotNull(node);
        }
    }
}