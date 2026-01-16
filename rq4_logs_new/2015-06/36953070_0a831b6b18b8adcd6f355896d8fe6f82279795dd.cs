using System;
using FactoryGirlCore;
using FactoryGirlDemo.Support.Factories;
using Models;
using NUnit.Framework;

namespace FactoryGirlDemo
{
    [TestFixture]
    public class ConstructorTests
    {
        private VehicleModel vehicle;

        [TestFixtureSetUp]
        public void BeforeAll()
        {
            FactoryGirl.Initialize(typeof(VehicleModelFactory));
        }

        [SetUp]
        public void BeforeEach()
        {
            vehicle = FactoryGirl.Build<VehicleModel>();
        }

        [Test]
        public void VehicleModel_Constructor_HasTire()
        {
            Assert.IsNotNull(vehicle.Tire);
        }

        [Test]
        public void VehicleModel_Constructor_ReadOnlyFieldPopulation()
        {
            Assert.IsNotNull(vehicle.GrossVehicleWeight);
        }
    }
}