using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using NSubstitute;
using Studio_Inventory_API.Controllers;
using Studio_Inventory_API.Models;
using Studio_Inventory_API.Repositories;


namespace Studio_Inventory_API.Tests
{
    public class CategoryControllerTests
    {
        CategoryController sut;
        IRepository<Category> categoryRepo;

        public CategoryControllerTests()
        {
            categoryRepo = Substitute.For<IRepository<Category>>();
            sut = new CategoryController(categoryRepo);
        }
    }
}