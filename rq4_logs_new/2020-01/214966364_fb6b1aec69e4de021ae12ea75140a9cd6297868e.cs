using System;
using System.Collections.Generic;
using DataLayer.SqlConnection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceLayer;

namespace Tests
{
    [TestClass]
    public class Repository
    {
        [TestClass]
        public class DataRepositoryTest
        {
            [TestMethod]
            public void GetAllProductCategoryTest()
            {
                IDataRepository dataRepository = new DataRepository();
                List<ProductCategory> productCategories = dataRepository.GetAllProductCategories();
                Assert.AreEqual(4, productCategories.Count);
            }

            [TestMethod]
            public void AddProductCategoryTest()
            {
                IDataRepository dataRepository = new DataRepository();
                ProductCategory productCategory = new ProductCategory
                {
                    Name = "Kamil",
                    ModifiedDate = DateTime.Today
                };

                dataRepository.AddProductCategory(productCategory);
                List<ProductCategory> productCategories = dataRepository.GetAllProductCategories();
                Assert.AreEqual(5, productCategories.Count);
            }

            [TestMethod]
            public void DeleteProductCategoryTest()
            {
                IDataRepository dataRepository = new DataRepository();
                dataRepository.DeleteProductCategory(5);
                List<ProductCategory> productCategories = dataRepository.GetAllProductCategories();
                Assert.AreEqual(4, productCategories.Count);
            }


            [TestMethod]
            public void UpdateUpdateProductCategory()
            {
                IDataRepository dataRepository = new DataRepository();
                dataRepository.UpdateProductCategory("Mirek", 1);
                List<ProductCategory> scrapReasons = dataRepository.GetAllProductCategories();
                Assert.AreEqual("Mirek", scrapReasons[0].Name);
            }
        }
    }
}