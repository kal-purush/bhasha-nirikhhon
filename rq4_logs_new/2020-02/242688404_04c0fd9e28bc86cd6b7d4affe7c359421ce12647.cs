using NUnit.Framework;
using System.Collections.Generic;

namespace SalesTaxes.Tests
{
    [TestFixture]
    public class Tests
    {
        private ICollection<IGood> _good;

        [SetUp]
        public void Setup()
        {
            _good = new List<IGood>() { 
                new Good() { Id=1, Name="book" , Imported = false, Price = 12.49f, Quantity=1 },
                new Good() { Id=2, Name="music" , Imported = false, Price = 14.99f, Quantity=1 },
                new Good() { Id=3, Name="chocolate bar" , Imported = false, Price = 0.85f, Quantity=1 }
            };
        }

        [Test]
        public void ShouldNotShoppingBasketBeEmpty()
        {
            Assert.IsTrue(_good.Count > 0);
        }

        [TearDown]
        public void AfterEachTest()
        {
            _good = null;
        }

    }
}