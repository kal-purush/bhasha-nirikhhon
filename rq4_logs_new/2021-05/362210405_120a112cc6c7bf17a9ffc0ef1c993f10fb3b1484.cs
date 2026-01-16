using Microsoft.VisualStudio.TestTools.UnitTesting;
using RDFSharp.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace RDFSharp.UnitTests
{
    [TestClass]
    public class RDFTripleTest
    {
        private RDFResource subjectResource = new RDFResource("https://en.wikipedia.org/wiki/Elizabeth_II");
        private RDFResource predicateResource = new RDFResource("http://xmlns.com/foaf/0.1/age");
        private RDFResource blankPredicateResource = new RDFResource();
        private RDFTypedLiteral objectLiteral = new RDFTypedLiteral("95", RDFModelEnums.RDFDatatypes.XSD_INTEGER);

        public string getDesiredOutputStatment(RDFResource subject, RDFResource predicate, RDFLiteral obj)
        {
            return subject.ToString() + " " + predicate.ToString() + " " + obj.ToString();
        }

        [TestMethod]
        public void RDFTriple_ValidResources_ReturnsSameWithToString()
        {
            //Arrange
            var rdf = new RDFTriple(subjectResource, predicateResource, objectLiteral);
            var desiredOutput = getDesiredOutputStatment(subjectResource, predicateResource, objectLiteral);

            //Act
            var result = rdf.ToString();
            Console.WriteLine(result);

            //Assert
            Assert.AreEqual(desiredOutput, result);
        }

        [TestMethod]
        public void RDFTriple_BlankResource_ThrowsRDFModelException()
        {
            //Arrange - Act
            Action actual = () => new RDFTriple(subjectResource, blankPredicateResource, objectLiteral);

            //Assert
            Assert.ThrowsException<RDFModelException>(actual);
        }
    }
}