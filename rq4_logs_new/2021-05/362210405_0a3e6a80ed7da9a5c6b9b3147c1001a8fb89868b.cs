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
    public class RDFOntologyDataTest
    {

        //[TestInitialize]
        public void setup() {
            //TODO
        }


        [TestMethod]
        public void Add_ExistingFact_Failure() {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            
            //Act
            ont.Data.AddFact(ontologyFact)
                    .AddFact(ontologyFact);

            //Assert
            Assert.AreEqual(1, ont.Data.FactsCount);
        }


        [TestMethod]
        public void Add_ExistingLiteral_Failure()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyLiteral = new RDFOntologyLiteral(new RDFPlainLiteral("Garfield"));


            //Act
            ont.Data.AddLiteral(ontologyLiteral)
                    .AddLiteral(ontologyLiteral);


            //Assert
            Assert.AreEqual(1, ont.Data.LiteralsCount);
        }

        [TestMethod]
        public void Add_SameAs_Relation_Success() {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var originalOntologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            var differentOntologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/orangeFunnyCat"));

            ont.Data.AddFact(originalOntologyFact)
                    .AddFact(differentOntologyFact);

            //Act
            ont.Data.AddSameAsRelation(differentOntologyFact, originalOntologyFact);

            //Assert
            Assert.IsTrue(ont.Data.CheckIsSameFactAs(originalOntologyFact, differentOntologyFact));

        }

        [TestMethod]
        public void Add_DifferentFrom_Relation_Success() {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var originalOntologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            var newOntologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/john"));

            ont.Data.AddFact(originalOntologyFact)
                    .AddFact(newOntologyFact);

            //Act
            ont.Data.AddDifferentFromRelation(originalOntologyFact, newOntologyFact);

            //Assert
            Assert.IsTrue(ont.Data.CheckIsDifferentFactFrom(originalOntologyFact, newOntologyFact));
        }

        [TestMethod]
        public void Add_Assertion_Success() {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            var ontologyLiteral = new RDFOntologyLiteral(new RDFPlainLiteral("Garfield"));
            var hasName = new RDFOntologyDatatypeProperty(new RDFResource("http://ont/props/hasName"));
            ont.Data.AddFact(ontologyFact);
            ont.Model.PropertyModel.AddProperty(hasName);

            //Act
            ont.Data.AddAssertionRelation(ontologyFact, hasName, ontologyLiteral);

            //Assert
            Assert.IsTrue(ont.Data.CheckIsAssertion(ontologyFact, hasName, ontologyLiteral));
        }

        [TestMethod]
        public void RemoveFact_Success() {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            var ontologyFact2 = new RDFOntologyFact(new RDFResource("http://ont/facts/john"));
            ont.Data.AddFact(ontologyFact)
                    .AddFact(ontologyFact2);

            //Act
            ont.Data.RemoveFact(ontologyFact);

            //Assert
            Assert.AreEqual(1, ont.Data.FactsCount);
        }


        [TestMethod]
        public void RenameFact_Success() {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            var ontologyFact2 = new RDFOntologyFact(new RDFResource("http://ont/facts/odie"));
            ont.Data.AddFact(ontologyFact)
                    .AddFact(ontologyFact2);
            var newOntologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/john"));

            //Act
            ont.Data.RenameFact(ontologyFact, newOntologyFact);

            //Assert
            var renamedFact = ont.Data.SelectFact("http://ont/facts/john");
            Assert.AreEqual(renamedFact, newOntologyFact);
        }

        [TestMethod]
        public void Remove_Literal_Success()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyLiteral = new RDFOntologyLiteral(new RDFPlainLiteral("Garfield"));
            var ontologyLiteral2 = new RDFOntologyLiteral(new RDFPlainLiteral("John"));
            ont.Data.AddLiteral(ontologyLiteral)
                    .AddLiteral(ontologyLiteral2);

            //Act
            ont.Data.RemoveLiteral(ontologyLiteral);

            //Assert
            Assert.AreEqual(1, ont.Data.LiteralsCount);
        }

        [TestMethod]
        public void Remove_NotExistingLiteral_Failure()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyLiteral = new RDFOntologyLiteral(new RDFPlainLiteral("Garfield"));
            var ontologyLiteral2 = new RDFOntologyLiteral(new RDFPlainLiteral("John"));
            ont.Data.AddLiteral(ontologyLiteral);
                
            //Act
            ont.Data.RemoveLiteral(ontologyLiteral2);

            //Assert
            Assert.AreEqual(1, ont.Data.LiteralsCount);
        }

        [TestMethod]
        public void Remove_SameAs_Relation_Success()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var originalOntologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            var differentOntologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/orangeFunnyCat"));
            ont.Data.AddFact(originalOntologyFact)
                    .AddFact(differentOntologyFact);
            ont.Data.AddSameAsRelation(differentOntologyFact, originalOntologyFact);

            //Act
            ont.Data.RemoveSameAsRelation(differentOntologyFact, originalOntologyFact);

            //Assert
            Assert.IsFalse(ont.Data.CheckIsSameFactAs(originalOntologyFact, differentOntologyFact));

        }

        [TestMethod]
        public void Remove_Assertion_Success()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var ontologyFact = new RDFOntologyFact(new RDFResource("http://ont/facts/garfield"));
            var ontologyLiteral = new RDFOntologyLiteral(new RDFPlainLiteral("Garfield"));
            var ontologyFact2 = new RDFOntologyFact(new RDFResource("http://ont/facts/john"));
            var ontologyLiteral2 = new RDFOntologyLiteral(new RDFPlainLiteral("John"));
            var hasName = new RDFOntologyDatatypeProperty(new RDFResource("http://ont/props/hasName"));
            ont.Data.AddFact(ontologyFact);
            ont.Data.AddFact(ontologyFact2);
            ont.Model.PropertyModel.AddProperty(hasName);
            ont.Data.AddAssertionRelation(ontologyFact, hasName, ontologyLiteral);
            ont.Data.AddAssertionRelation(ontologyFact2, hasName, ontologyLiteral2);

            //Act
            ont.Data.RemoveAssertionRelation(ontologyFact, hasName, ontologyLiteral);

            //Assert
            Assert.IsFalse(ont.Data.CheckIsAssertion(ontologyFact, hasName, ontologyLiteral));
        }

    }
}