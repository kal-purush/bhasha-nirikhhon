using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WebStore.Interfaces.Services;
using Moq;
using WebStore.Domain.DTO.Products;
using WebStore.Controllers;
using Microsoft.AspNetCore.Mvc;
using WebStore.Domain.ViewModels;
using WebStore.Domain;

namespace WebStore.UI.Tests.Controllers
{
    [TestClass]
    public class CatalogueControllerTests
    {
        [TestMethod]
        public void Details_Returns_With_Correct_View() {
            //Arange
            var product_data_mock = new Mock<IProductData>();

            const int expected_product_id = 1;
            const decimal expected_product_price = 10m;

            var expected_product_name = $"Name_{expected_product_id}";
            var expected_product_image = $"img{expected_product_id}.png";
            var expected_brand_name = $"Brand of product {expected_product_id}";
            var expected_section_name = $"Section of product {expected_product_id}";

            product_data_mock.Setup(p => p.GetProductById(It.IsAny<int>()))
                .Returns<int>(id => new ProductDTO(
                    id,
                    $"Name_{id}",
                    1,
                    expected_product_price,
                    $"img{id}.png",
                    new BrandDTO(1, $"Brand of product {id}", 1, 1),
                    new SectionDTO(1, $"Section of product {id}", 1, null)
                    ));

            //Act
            var controller = new CatalogueController(product_data_mock.Object);

            //Assert
            Assert.IsInstanceOfType(controller.Details(expected_product_id), typeof(ViewResult));
            var model = (controller.Details(expected_product_id) as ViewResult).Model as ProductViewModel;
            Assert.AreEqual(model.Id, expected_product_id);
            Assert.AreEqual(model.Name, expected_product_name);
            Assert.AreEqual(model.Price, expected_product_price);
            Assert.AreEqual(model.ImageUrl, expected_product_image);
            Assert.AreEqual(model.Brand, expected_brand_name);            
        }

        [TestMethod]
        public void Shop_Returns_With_Correct_View()
        {
            //Arange
            var product_data_mock = new Mock<IProductData>();

            var products = new[] {
                new ProductDTO(1, "Product_1", 1, 10m, "img_1.png", new BrandDTO(1, "Brand_1", 1, 1), new SectionDTO(1, "Section_1", 1, null)),
                new ProductDTO(2, "Product_2", 1, 20m, "img_2.png", new BrandDTO(2, "Brand_2", 2, 1), new SectionDTO(2, "Section_2", 2, null)),
                new ProductDTO(3, "Product_3", 1, 30m, "img_3.png", new BrandDTO(3, "Brand_3", 3, 1), new SectionDTO(3, "Section_3", 3, null)),
            };

            int expected_product_count = 3;
            int expected_brand_id = 4;
            int expected_section_id = 2;

            product_data_mock.Setup(p => p.GetProducts(It.IsAny<ProductFilter>())).Returns(() => products);

            //Act
            CatalogueController controller = new CatalogueController(product_data_mock.Object);

            //Assert
            ViewResult viewResult = Xunit.Assert.IsType<ViewResult>(controller.Shop(expected_brand_id, expected_section_id));

            var model = Xunit.Assert.IsAssignableFrom<CatalogueViewModel>(viewResult.Model);
            Assert.AreEqual(expected_product_count, model.Products.ToList().Count);
            Assert.AreEqual(expected_brand_id, model.BrandId);
            Assert.AreEqual(expected_section_id, model.SectionId);
            
        }
    }
}