using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GalaSoft.MvvmLight.Messaging;
using Moq;
using NUnit.Framework;
using SampleApp.Data;
using SampleApp.Messages;
using SampleApp.Model;
using SampleApp.ViewModel;

namespace SampleApp.Tests.Model
{
    [TestFixture]
    public class BasketModelTests
    {
        private BasketModel _model;

        [SetUp]
        public void Setup()
        {
            _model = new BasketModel();
        }

        [Test]
        public void ShouldAddProductWhenRecievingAddProductEvent()
        {
            var product = new Product();
            _model.AddItem(product);

            Assert.AreEqual(1, _model.Items.Count());
            Assert.AreEqual(product, _model.Items.First().Product);
        }

        [Test]
        public void ShouldIncrementProductCountWhenRecievingAddProductEventForExistingProduct()
        {
            var product1 = new Product() { ProductId = 1 };
            var product2 = new Product() { ProductId = 1 };

            _model.AddItem(product1);
            _model.AddItem(product2);

            Assert.AreEqual(1, _model.Items.Count());

            var productViewModel = _model.Items.First();

            Assert.AreEqual(product1, productViewModel.Product);
            Assert.AreEqual(2, productViewModel.Count);
        }

        [Test]
        public void ShouldUpdateTotalToReflectPriceOfItems()
        {
            var product1 = new Product() { ProductId = 1, Price = 1 };
            var product2 = new Product() { ProductId = 1, Price = 1 };
            var product3 = new Product() { ProductId = 2, Price = 2 };

            _model.AddItem(product1);
            _model.AddItem(product2);
            _model.AddItem(product3);

            Assert.AreEqual(4, _model.Total);
        }
    }
}