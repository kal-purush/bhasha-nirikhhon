using ProductsAPI.Models;
using ProductsAPI.Repository;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ProductsAPI.Tests
{
    public class RepositoryTests
    {
        [Fact]
        public void GetProductById_Returns_Product()
        {
            var id = Guid.Empty;
            var productsRepository = new ProductsRepository();
            var product = productsRepository.GetById(id);

            Assert.IsType<Product>(product);
            Assert.Equal(id, product.Id);
        }
    }
}