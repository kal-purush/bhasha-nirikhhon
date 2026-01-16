using GraphBreadthFirst;
using System.Xml.Linq;

namespace TestBreadth
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {

        }
        [Fact]
        public void BreadthFirstExpectedOrder()
        {
            // Arrange
            Node pandora = new Node("Pandora");
            Node arendelle = new Node("Arendelle");
            Node metroville = new Node("Metroville");
            Node monstroplolis = new Node("Monstroplolis");
            Node narnia = new Node("Narnia");
            Node naboo = new Node("Naboo");

            pandora.Neighbors.Add(arendelle);
            arendelle.Neighbors.Add(pandora);
            arendelle.Neighbors.Add(metroville);
            metroville.Neighbors.Add(arendelle);
            metroville.Neighbors.Add(monstroplolis);
            monstroplolis.Neighbors.Add(metroville);
            monstroplolis.Neighbors.Add(narnia);
            narnia.Neighbors.Add(monstroplolis);
            narnia.Neighbors.Add(naboo);
            naboo.Neighbors.Add(narnia);

            Graph graph = new Graph(pandora);

            // Act
            List<Node> bfsResult = graph.BreadthFirst(pandora);

            // Assert
            Assert.Equal("Pandora, Arendelle, Metroville, Monstroplolis, Narnia, Naboo", string.Join(", ", bfsResult.Select(node => node.Value)));
        }

        [Fact]
        public void WhenPathExists()
        {
            // Arrange
            Node pandora = new Node("Pandora");
            Node arendelle = new Node("Arendelle");
            Node metroville = new Node("Metroville");
            Node monstroplolis = new Node("Monstroplolis");

            pandora.Neighbors.Add(arendelle);
            arendelle.Neighbors.Add(pandora);
            arendelle.Neighbors.Add(metroville);
            metroville.Neighbors.Add(arendelle);
            metroville.Neighbors.Add(monstroplolis);
            monstroplolis.Neighbors.Add(metroville);

            Graph graph = new Graph(pandora);

            // Act
            bool hasPath = graph.HasPath(pandora, monstroplolis);

            // Assert
            Assert.True(hasPath);
        }

        [Fact]
        public void WhenPathDoesNotExist()
        {
            // Arrange
            Node pandora = new Node("Pandora");
            Node arendelle = new Node("Arendelle");
            Node metroville = new Node("Metroville");
            Node monstroplolis = new Node("Monstroplolis");
            Node narnia = new Node("Narnia");
            Node naboo = new Node("Naboo");
            Node Amman = new Node("Amman");
            pandora.Neighbors.Add(arendelle);
            arendelle.Neighbors.Add(pandora);
            arendelle.Neighbors.Add(metroville);
            metroville.Neighbors.Add(arendelle);
            metroville.Neighbors.Add(monstroplolis);
            monstroplolis.Neighbors.Add(metroville);
            monstroplolis.Neighbors.Add(narnia);
            narnia.Neighbors.Add(monstroplolis);
            narnia.Neighbors.Add(naboo);

            Graph graph = new Graph(pandora);

            // Act
            bool hasPath = graph.HasPath(pandora, Amman);

            // Assert
            Assert.False(hasPath);
        }
    }
}