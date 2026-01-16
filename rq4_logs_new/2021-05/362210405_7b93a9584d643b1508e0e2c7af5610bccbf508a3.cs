using Microsoft.VisualStudio.TestTools.UnitTesting;
using RDFSharp.Model;
using RDFSharp.Semantics.OWL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RDFSharp.UnitTests
{
    [TestClass]
    public class RDFOntologyFactTest
    {
        [TestMethod]
        public void CreateOntologyFact_Success() {
            //Arrange
            var resource = new RDFResource("http://ont/facts/garfield");


            //Act
            var ontologyFact = new RDFOntologyFact(resource);

            //Assert
            Assert.AreEqual(ontologyFact.Value, resource);
        }

        [TestMethod]
        [ExpectedException(typeof(RDFSemanticsException))]
        public void CreateOntologyFact_Failure()
        {
            //Arrange
            RDFResource resource = null;

            //Act
            var ontologyFact = new RDFOntologyFact(resource);

            
        }
    }
}