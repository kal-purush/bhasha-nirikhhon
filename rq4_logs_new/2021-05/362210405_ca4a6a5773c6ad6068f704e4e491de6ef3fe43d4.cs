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
    public class RDFOntologyTest
    {
        [TestMethod]
        public void UnionWith_hasSameNumberOfClassesAndProperties()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var siberian = new RDFOntologyClass(new RDFResource("http://ont/classes/siberian"));
            ont.Model.ClassModel.AddClass(siberian);
            var hasName = new RDFOntologyDatatypeProperty(new RDFResource("http://ont/props/hasName"));
            ont.Model.PropertyModel.AddProperty(hasName);

            var new_ont = new RDFOntology(new RDFResource("http://new_ont/"));
            var persian = new RDFOntologyClass(new RDFResource("http://ont/classes/persian"));
            new_ont.Model.ClassModel.AddClass(persian);
            var hasNote = new RDFOntologyAnnotationProperty(new RDFResource("http://ont/props/hasNote"));
            new_ont.Model.PropertyModel.AddProperty(hasNote);

            //Act
            var union_ont = ont.UnionWith(new_ont);

            //Assert
            Assert.AreEqual(2, union_ont.Model.ClassModel.ClassesCount);
            Assert.AreEqual(1, union_ont.Model.PropertyModel.AnnotationPropertiesCount);
            Assert.AreEqual(1, union_ont.Model.PropertyModel.DatatypePropertiesCount);
        }

        [TestMethod]
        public void IntersectWith_hasSameNumberOfClasses()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var siberian = new RDFOntologyClass(new RDFResource("http://ont/classes/siberian"));
            ont.Model.ClassModel.AddClass(siberian);
            var hasName = new RDFOntologyDatatypeProperty(new RDFResource("http://ont/props/hasName"));
            ont.Model.PropertyModel.AddProperty(hasName);
            var persian = new RDFOntologyClass(new RDFResource("http://ont/classes/persian"));
            ont.Model.ClassModel.AddClass(persian);

            var new_ont = new RDFOntology(new RDFResource("http://new_ont/"));
            new_ont.Model.ClassModel.AddClass(siberian);
            new_ont.Model.ClassModel.AddClass(persian);

            //Act
            var common_ont = ont.IntersectWith(new_ont);

            //Assert
            Assert.AreEqual(2, common_ont.Model.ClassModel.ClassesCount);
            Assert.AreEqual(0, common_ont.Model.PropertyModel.DatatypePropertiesCount);
        }

        [TestMethod]
        public void DifferenceWith_hasSameNumberOfClasses()
        {
            //Arrange
            var ont = new RDFOntology(new RDFResource("http://ont/"));
            var siberian = new RDFOntologyClass(new RDFResource("http://ont/classes/siberian"));
            ont.Model.ClassModel.AddClass(siberian);
            var hasName = new RDFOntologyDatatypeProperty(new RDFResource("http://ont/props/hasName"));
            ont.Model.PropertyModel.AddProperty(hasName);
            var persian = new RDFOntologyClass(new RDFResource("http://ont/classes/persian"));
            ont.Model.ClassModel.AddClass(persian);

            var new_ont = new RDFOntology(new RDFResource("http://new_ont/"));
            new_ont.Model.ClassModel.AddClass(persian);

            //Act
            var difference_ont = ont.DifferenceWith(new_ont);

            //Assert
            Assert.AreEqual(1, difference_ont.Model.ClassModel.ClassesCount);
            Assert.AreEqual(1, difference_ont.Model.PropertyModel.DatatypePropertiesCount);
        }
    }
}