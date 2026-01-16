using Data.Design;
using Domain.DAO;
using NUnit.Framework;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;
using Domain.Entities.Products;
using Data.EFRepository;
using Data.StoreData;

namespace Test.EfDao
{
    [TestFixture]
    class EfCategoryTest
    {
        IQueryable<Category> data;
        Mock<DbSet<Category>> mockSet;
        Mock<ProjectContext> mockContext;
        ICategoryRepository service;


        [SetUp]
        public void Init()
        {
            data = new List<Category>
            {
                new Category(){ Id = 1, IsActive = true,  Title = "Wooden",   Date = DateTime.Now.AddDays(-20)},
                new Category(){ Id = 2, IsActive = false, Title = "Modern",   Date = DateTime.Now.AddDays(-10)},
                new Category(){ Id = 3, IsActive = true,  Title = "Metal",    Date = DateTime.Now.AddDays(-15)}
            }.AsQueryable();

            mockSet = new Mock<DbSet<Category>>();
            mockSet.As<IQueryable<Category>>().Setup(m => m.Provider).Returns(data.Provider);
            mockSet.As<IQueryable<Category>>().Setup(m => m.Expression).Returns(data.Expression);
            mockSet.As<IQueryable<Category>>().Setup(m => m.ElementType).Returns(data.ElementType);
            mockSet.As<IQueryable<Category>>().Setup(m => m.GetEnumerator()).Returns(data.GetEnumerator());

            mockContext = new Mock<ProjectContext>();
            mockContext.Setup(x => x.Categories).Returns(mockSet.Object);

            service = new EFCategoryDAO(mockContext.Object);
        }


        [Test]
        public void AddCategoryTest()
        {
            service.Create(new Category (){ Title = "Pretty", Id = 4, IsActive = true, Date = DateTime.Now });
            mockSet.Verify(m => m.Add(It.IsAny<Category>()), Times.Once());
            mockContext.Verify(m => m.SaveChanges(), Times.Once());
        }

        [Test]
        public void GetAllCategoriesTest()
        {
            var categories = service.GetAll().ToList();

            Assert.AreEqual(3, categories.Count());
            Assert.AreEqual(categories[0].Title, "Wooden");
            Assert.AreEqual(categories[1].Title, "Modern");
            Assert.AreEqual(categories[2].Title, "Metal");
        }

        [Test]
        public void GetActiveCategoryTest()
        {
            var listData = data.ToList();
            var expected = new List<Category> { listData[0], listData[2] };
            var activeCategories = service.GetByIsActive(true).ToList();
            CollectionAssert.AreEqual(expected, activeCategories);
        }

        [Test]
        public void GetUnActiveCategoryTest()
        {
            var listData = data.ToList();
            var expected = new List<Category> { listData[1]};
            var activeCategories = service.GetByIsActive(false).ToList();
            CollectionAssert.AreEqual(expected, activeCategories);
        }

        [TestCase(1, Result = "Wooden", TestName = "Get Wooden Category")]
        [TestCase(2, Result = "Modern", TestName = "Get Modern Category")]
        [TestCase(0, ExpectedException = typeof(NullReferenceException), TestName = "Id doesn't exist", Description = "NullReferenceException is expected")]
        public string ReadCategoryTest(int id)
        {
            var category = service.Read(id);
            return category.Title;
        }
    }
}