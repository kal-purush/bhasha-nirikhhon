using System;
using Xunit;
using System.Linq;
using ToDoApi.Data;
using Microsoft.EntityFrameworkCore;
using ToDoApi.Models;
using ToDoApi.Controllers;
using System.Net;
using Microsoft.AspNetCore.Mvc;

namespace ToDoApiTests
{
    public class ToDoControllerTest
    {
        [Fact]
        public void CanCreateAToDoItem()
        {
            DbContextOptions<ToDoDbContext> options =
                new DbContextOptionsBuilder<ToDoDbContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;

            using (ToDoDbContext context = new ToDoDbContext(options))
            {
                ToDoItem item = new ToDoItem
                {
                    Name = "test todo",
                    IsDone = true
                };

                ToDoController tdc = new ToDoController(context);
                var result = tdc.Create(item).Result;
                var r = (ObjectResult)result;
                var status = r.StatusCode.Value;

                Assert.Equal(HttpStatusCode.Created, (HttpStatusCode)status);
            }
        }
        [Fact]
        public async void CanReadAToDoItem()
        {
            DbContextOptions<ToDoDbContext> options =
                new DbContextOptionsBuilder<ToDoDbContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;

            using (ToDoDbContext context = new ToDoDbContext(options))
            {

                ToDoItem item = new ToDoItem
                {
                    Name = "test todo",
                    IsDone = true,
                    ListID = 1
                };
                ToDoList list = new ToDoList
                {
                    ID = 1,
                    Name = "test list",
                    IsDone = true
                };

                ToDoListController tdlc = new ToDoListController(context);
                await tdlc.Create(list);
                ToDoController tdc = new ToDoController(context);
                await tdc.Create(item);

                var findItem = await tdc.GetById(item.ID);
                var result = (ObjectResult)findItem.Result;
                var readItem = (ToDoItem)result.Value;


                Assert.Equal(item.Name, readItem.Name);
            }
        }
        [Fact]
        public async void CanUpdateAToDoItem()
        {
            DbContextOptions<ToDoDbContext> options =
                new DbContextOptionsBuilder<ToDoDbContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;

            using (ToDoDbContext context = new ToDoDbContext(options))
            {

                ToDoItem item = new ToDoItem
                {
                    Name = "test todo",
                    IsDone = true,
                    ListID = 1
                };
                ToDoList list = new ToDoList
                {
                    ID = 1,
                    Name = "test list",
                    IsDone = true
                };

                ToDoListController tdlc = new ToDoListController(context);
                await tdlc.Create(list);
                ToDoController tdc = new ToDoController(context);
                await tdc.Create(item);

                ToDoItem update = new ToDoItem
                {
                    ID = item.ID,
                    Name = "todo test",
                    IsDone = false,
                    ListID = item.ListID
                };

                await tdc.Update(item.ID, update);

                var findItem = await tdc.GetById(update.ID);
                var result = (ObjectResult)findItem.Result;
                var readItem = (ToDoItem)result.Value;


                Assert.Equal(update.Name, readItem.Name);
            }
        }
        [Fact]
        public void CanDeleteAToDoItem()
        {
            DbContextOptions<ToDoDbContext> options =
                new DbContextOptionsBuilder<ToDoDbContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;

            using (ToDoDbContext context = new ToDoDbContext(options))
            {

                ToDoItem item = new ToDoItem
                {
                    ID = 1,
                    Name = "test todo",
                    IsDone = true
                };

                ToDoController tdc = new ToDoController(context);
                var result = tdc.Create(item).Result;

                var results = context.ToDoItems.Where(i => i.Name == "test todo");
                Assert.Equal(1, results.Count());

                var remove = tdc.Delete(item.ID);
                Assert.True(remove.IsCompletedSuccessfully);
            }
            
        }
    }
}