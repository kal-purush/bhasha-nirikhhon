using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Studio_Inventory_API.Models;

namespace Studio_Inventory_API.Tests
{
    public class CategoryTests
    {
        Category sut;

        public CategoryTests()
        {
            sut = new Category();
            sut.Id = 999;
            sut.Name = "";
        }

        [Fact]
        public void Category_Constructor_Sets_Id_On_CategoryModel()
        {
            //Arrange

            //Act
            int id = sut.Id;

            //Assert
            Assert.Equal(999, id);
        }

        [Fact]
        public void Category_Constructor_Sets_Name_On_CategoryModel()
        {
            string name = sut.Name;
            Assert.Equal("", name);
        }

    }
}