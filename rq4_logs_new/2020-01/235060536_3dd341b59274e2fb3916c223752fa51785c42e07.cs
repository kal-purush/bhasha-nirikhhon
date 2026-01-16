using System;
using NUnit.Framework;

namespace TaxCalculator.Tests
{
    public class TaxCalculatorForRDE2CompliantDieselTests
    {
        private static readonly DateTime FirstOfJanuary2019 = new DateTime(2019, 1, 1);
        private TaxCalculator _taxCalculator;

        [SetUp]
        public void BeforeEach()
        {
            _taxCalculator = new DefaultTaxCalculator();
        }
        
        [Test]
        public void WhenVehicleHas0GramsCo2()
        {
            var vehicle = new Vehicle(0, FuelType.Diesel, FirstOfJanuary2019, 20000, true);
            var tax = _taxCalculator.CalculateTax(vehicle);
            Assert.AreEqual(0, tax);
        }
        
        [Test]
        public void WhenVehicleHas110GramsCo2()
        {
            var vehicle = new Vehicle(110, FuelType.Diesel, FirstOfJanuary2019, 20000, true);
            var tax = _taxCalculator.CalculateTax(vehicle);
            Assert.AreEqual(150, tax);
        }

    }
}