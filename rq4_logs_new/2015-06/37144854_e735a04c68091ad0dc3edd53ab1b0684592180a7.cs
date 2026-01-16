using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DesignPatterns.OOP.Encapsulation.Vehicles;

namespace DesignPatterns.Test.TDD.ConceptsOOP.Encapsulation
{
    [TestClass]
    public class CarTest
    {
        [TestMethod]
        [TestCategory("Encapsulation")]
        public void Validate_Move_Car()
        {
            string expectedMove = "Starting engine, injecting gasoline...";
            var corsa = new Car();
            string move = corsa.Move();

            Assert.AreEqual(expectedMove, move);
        }
    }
}