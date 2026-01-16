using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Moq;
using System.Collections.Generic;
using System.Linq;
using ToDoListApp.Controllers;
using ToDoListApp.DatabaseContext;
using ToDoListApp.Models;
using ToDoListApp.Services;

namespace ToDoUnitTest
{
    public class ToDoControllerTest
    {
        private readonly IQueryable<ToDoItemModel> _itemData;
        private readonly IToDoDbContext _mockDbContext;

        public ToDoControllerTest()
        {
            _itemData = new List<ToDoItemModel>
            {
                new ToDoItemModel
                {
                    Id = 1,
                    Name = "Create REST API",
                    IsComplete = true
                },
                new ToDoItemModel
                {
                    Id = 2,
                    Name = "Add JWT authentication",
                    IsComplete = true
                }
            }.AsQueryable();

            var mockSet = new Mock<DbSet<ToDoItemModel>>();
            mockSet.As<IQueryable<ToDoItemModel>>().Setup(m => m.Provider).Returns(_itemData.Provider);
            mockSet.As<IQueryable<ToDoItemModel>>().Setup(m => m.Expression).Returns(_itemData.Expression);
            mockSet.As<IQueryable<ToDoItemModel>>().Setup(m => m.ElementType).Returns(_itemData.ElementType);
            mockSet.As<IQueryable<ToDoItemModel>>().Setup(m => m.GetEnumerator()).Returns(_itemData.GetEnumerator());

            var mockContext = new Mock<IToDoDbContext>();
            mockContext.Setup(c => c.ToDoItems).Returns(mockSet.Object);
            _mockDbContext = mockContext.Object;
        }

        [Fact]
        public void GetAllItemsTest()
        {
            var toDoService = new ToDoService(_mockDbContext);

            var result = toDoService.GetAllItems();
            Assert.IsType<List<ToDoItemModel>>(result);
            Assert.Equal(2, result.Count);
        }

        [Fact]
        public void GetItemById()
        {
            var toDoService = new ToDoService(_mockDbContext);

            var getId1 = toDoService.GetById(1);
            Assert.IsType<ToDoItemModel>(getId1);
            Assert.Equal("Create REST API", getId1.Name);

            var getId2 = toDoService.GetById(2);
            Assert.IsType<ToDoItemModel>(getId2);
            Assert.Equal("Add JWT authentication", getId2.Name);

        }

        [Fact]
        public void AddItemTest()
        {
            var toDoService = new ToDoService(_mockDbContext);
            var itemModel = new ToDoItemModel
            {
                Id = 3,
                Name = "Create unit testing",
                IsComplete = false,
            };
            toDoService.AddItem(itemModel);
            
            var result = toDoService.GetById(3);
            Assert.IsType<ToDoItemModel>(result);
            Assert.Equal("Create unit testing", result.Name);
        }

        [Fact]
        public void UpdateItemTest()
        {
            var toDoService = new ToDoService(_mockDbContext);
            var updateRequest = new UpdateItemRequestModel
            {
                Name = "Push to git",
                IsComplete = true,
            };
            var result = toDoService.UpdateItem(2, updateRequest);
            Assert.IsType<ToDoItemModel>(result);
            Assert.Equal("Push to git", result.Name);
        }

        [Fact]
        public void DeleteItemTest()
        {
            var toDoService = new ToDoService(_mockDbContext);
            var itemModel = new ToDoItemModel
            {
                Id = 2,
                Name = "Add JWT authentication",
                IsComplete = true,
            };
            toDoService.DeleteItem(itemModel);

            var result = toDoService.GetAllItems();
            Assert.Equal(1, result.Count);
        }
    }
}