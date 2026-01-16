
using graph_depth_first;
namespace Testgraphdepth
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {

        }
        [Fact]
        public void SingleNode()
        {
            // Arrange
            Graph graph = new Graph();
            graph.AddNode(1);

            // Act
            var result = graph.DepthFirst(1);

            // Assert
            Assert.Collection(result, node => Assert.Equal(1, node));
        }

        [Fact]
        public void DisconnectedNodes()
        {
            // Arrange
            Graph graph = new Graph();
            graph.AddNode(1);
            graph.AddNode(2);
            graph.AddNode(3);

            // Act
            var result = graph.DepthFirst(1);

            // Assert
            Assert.Collection(result,
                node => Assert.Equal(1, node)
                
                );
        }

        [Fact]
        public void GivenConnectedGraph()
        {
            // Arrange
            Graph graph = new Graph();
            graph.AddNode(1);
            graph.AddNode(2);
            graph.AddNode(3);
            graph.AddNode(4);
            graph.AddEdge(1, 2);
            graph.AddEdge(1, 3);
            graph.AddEdge(2, 4);

            // Act
            var result = graph.DepthFirst(1);

            // Assert
            Assert.Collection(result,
                node => Assert.Equal(1, node),
                node => Assert.Equal(2, node),
                node => Assert.Equal(4, node),
                node => Assert.Equal(3, node));
        }
    }
}