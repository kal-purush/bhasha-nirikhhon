using KAryTree;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace KAryTest
{
    public class KAryNodeTest
    {
        [Fact]
        public void CanGetValue()
        {
            // Arrange
            const int nodeValue = 5;
            KAryNode<int> node = new KAryNode<int>() { Value = nodeValue };

            // Assert
            Assert.Equal(nodeValue, node.Value);
        }

        [Fact]
        public void CanSetValue()
        {
            // Arrange
            const int originalValue = 5;
            const int newValue = 10;

            KAryNode<int> node = new KAryNode<int>() { Value = originalValue };

            // Act
            node.Value = newValue;

            // Assert
            Assert.Equal(newValue, node.Value);
        }

        [Fact]
        public void CanGetChildren()
        {
            // Arrange
            KAryNode<int> node = new KAryNode<int>();

            // Assert
            Assert.Empty(node.Children);
        }
    }
}