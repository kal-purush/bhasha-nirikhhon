using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DesignPatterns.OOP.Polimorfism.Vehicles;

namespace DesignPatterns.Test.TDD.ConceptsOOP.Polimorfism
{
    [TestClass]
    public class BicycleTest
    {
        [TestMethod]
        [TestCategory("Polimorfism")]
        public void Validate_Move_Bicycle()
        {
            string expectedMove = "I move like a bicycle...";

            var bicycle = new Bicycle();
            string move = bicycle.Move();

            Assert.AreEqual(expectedMove, move);
        }
    }
}