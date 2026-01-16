using DPLRef.eCommerce.Accessors;
using DPLRef.eCommerce.Accessors.Sales;
using DPLRef.eCommerce.Common.Contracts;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace DPLRef.eCommerce.Tests.AccessorTests
{
    [TestClass]
    public class TaxAccessorTests
    {
        private ITaxRateAccessor CreateAccessor()
        {
            var context = new Common.Contracts.AmbientContext();
            var factory = new AccessorFactory(
                context,
                new Utilities.UtilityFactory(context));
            return factory.CreateAccessor<ITaxRateAccessor>();
        }

        [TestMethod]
        public void TaxAccessor_LincolnNe()
        {
            // arrange
            var accessor = CreateAccessor();
            var lincoln = new Address()
            {
                Postal = "68512"
            };

            // act
            var taxRate = accessor.Rate(lincoln);

            // assert
            Assert.AreEqual(0.0725m, taxRate);
        }

        [TestMethod]
        public void TaxAccessor_HooperNe()
        {
            // arrange
            var accessor = CreateAccessor();
            var hooper = new Address()
            {
                Postal = "68031"
            };

            // act
            var taxRate = accessor.Rate(hooper);

            // assert
            Assert.AreEqual(0.065m, taxRate);
        }


    }
}