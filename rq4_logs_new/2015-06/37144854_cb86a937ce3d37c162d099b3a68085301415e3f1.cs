using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DesignPatterns.BehavioralPatterns.Strategy.Animals;

namespace DesignPatterns.Test.TDD.BehavioralPatterns.Strategy
{
    [TestClass]
    public class AnimalsManagementTest
    {
        private AnimalsManagement _animalsManagementTest;

        public AnimalsManagementTest()
        {
            _animalsManagementTest = new AnimalsManagement();
        }

        [TestMethod]
        [TestCategory("Strategy")]
        public void Validate_Get_All_Animals()
        {
            List<Animal> animals = _animalsManagementTest.GetAll();

            Assert.IsNotNull(animals);
            Assert.IsTrue(animals.Count > 0);
        }
    }
}