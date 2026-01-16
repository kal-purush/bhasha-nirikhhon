using Models.SalesTaxes;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;

namespace SalesTaxes.Tests
{
    [TestFixture]
    public class Tests
    {
        private ICollection<IGood> _goods;

        [SetUp]
        public void Setup()
        {
            _goods = new List<IGood>() { 
                new Good() { Id=1, Name="book" , Imported = false, Price = 12.49f, Quantity=1, Category = Utilities.Utilities.Category.Books },
                new Good() { Id=2, Name="music" , Imported = false, Price = 14.99f, Quantity=1, Category = Utilities.Utilities.Category.MusicCDs },
                new Good() { Id=3, Name="chocolate bar" , Imported = false, Price = 0.85f, Quantity=1, Category = Utilities.Utilities.Category.Food }
            };
        }

        [Test]
        public void ShouldNotShoppingBasketBeEmpty()
        {
            Assert.IsTrue(_goods.Count > 0);
        }

        [Test]
        public void ShouldNotAddImportedTaxes()
        {
            Assert.IsFalse(_goods.Where(g => g.Imported == true).Count() > 0);
        }

        [TearDown]
        public void AfterEachTest()
        {
            _goods = null;
        }

    }
}