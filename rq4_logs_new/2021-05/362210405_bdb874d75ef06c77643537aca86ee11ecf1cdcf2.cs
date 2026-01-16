using Microsoft.VisualStudio.TestTools.UnitTesting;
using RDFSharp.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace RDFSharp.UnitTests
{
    [TestClass]
    public class RDFGraphTest
    {
        private string defaultNamespaceString = RDFNamespaceRegister.DefaultNamespace.ToString();
        private List<RDFTriple> startingTriples = TestRDFObjectProvider.triples.GetRange(0, 2);
        private RDFTriple newTriple = TestRDFObjectProvider.triples[2];

        [TestMethod]
        public void RDFGraph_DefaultCtor_ReturnsDefaultNamespaceWithToString()
        {
            //Arrange
            var graph = new RDFGraph();

            //Act
            var result = graph.ToString();

            //Assert
            Assert.AreEqual(defaultNamespaceString, result);
        }

        [TestMethod]
        public void Iterate_CreatedFromListOfTriples_ReturnsSameTriples()
        {
            //Arrange
            var graph = new RDFGraph(startingTriples);
            var desiredStoredTriples = new List<RDFTriple>(startingTriples);
            var storedTriples = new List<RDFTriple>();

            //Act
            foreach(var triple in graph)
            {
                storedTriples.Add(triple);
            }

            //Assert
            CollectionAssert.AreEqual(desiredStoredTriples, storedTriples);
        }

        private void getGraphStoredTriples(RDFGraph graph, ref List<RDFTriple> outputList)
        {
            foreach (var triple in graph)
            {
                outputList.Add(triple);
            }
        }

        [TestMethod]
        public void AddTriple_AddNewValidTriple_InsertsNewTriple()
        {
            //Arrange
            var graph = new RDFGraph(startingTriples);
            var desiredStoredTriples = startingTriples;
            desiredStoredTriples.Add(newTriple);
            var storedTriples = new List<RDFTriple>();

            //Act
            graph.AddTriple(newTriple);

            //Assert
            getGraphStoredTriples(graph, ref storedTriples);
            CollectionAssert.AreEqual(desiredStoredTriples, storedTriples);
        }

        [TestMethod]
        public void AddTriple_AddNullTriple_InsertsNothingWithoutException()
        {
            //Arrange
            var graph = new RDFGraph(startingTriples);
            var desiredStoredTriples = startingTriples;
            var storedTriples = new List<RDFTriple>();

            //Act
            graph.AddTriple(null);

            //Assert
            getGraphStoredTriples(graph, ref storedTriples);
            CollectionAssert.AreEqual(desiredStoredTriples, storedTriples);
        }

        [TestMethod]
        public void AddTriple_AddTripleAlreadyInList_InsertsNothingWithoutException()
        {
            //Arrange
            var graph = new RDFGraph(startingTriples);
            var desiredStoredTriples = startingTriples;
            var storedTriples = new List<RDFTriple>();

            //Act
            graph.AddTriple(startingTriples[0]);

            //Assert
            getGraphStoredTriples(graph, ref storedTriples);
            CollectionAssert.AreEqual(desiredStoredTriples, storedTriples);
        }
    }
}