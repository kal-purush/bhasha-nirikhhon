using System;
using BangazonSiteMVC.Controllers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using BangazonSiteMVC.Models;

namespace BangazonSiteMVC.Tests
{
    [TestClass]
    public class ProductTests
    {
        [TestMethod]
        public void AddingAProduct()
        {
            //Arrange
            var mockProductRepository = new Mock<IProductRepository>();
            var controller = new ProductController(mockProductRepository.Object);
            var newProduct = new Product
            {
                ProductId = 1,
                ProductName = "Trucker hat",
                ProductType = 2,
                Description = "Bringin' the 00s back!",
                CustomerId = 3
                
            };

            //Act
            controller.AddAProduct(newProduct);


            //Assert
            mockProductRepository.Verify(x => x.Save(newProduct),Times.Once);

        }
    }
}