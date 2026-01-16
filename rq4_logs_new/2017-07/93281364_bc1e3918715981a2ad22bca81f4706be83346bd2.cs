using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using NetSort.UnitTests.EF;
using System.Collections.Generic;
using System.Linq;

namespace NetSort.UnitTests
{
    [TestClass]
    public class IQueryableSortableExtensionsTests
    {
        [TestMethod]
        public void IQueryableSortableExtensionsTests_SimpleIQueryableSort_Works()
        {
            var data = new List<EF_Person>
            {
                new EF_Person { Age = 5, },
                new EF_Person { Age = 2, },
                new EF_Person { Age = 3, },
            }.AsQueryable();

            var mockSet = new Mock<DbSet<EF_Person>>();
            mockSet.As<IQueryable<EF_Person>>().Setup(m => m.Provider).Returns(data.Provider);
            mockSet.As<IQueryable<EF_Person>>().Setup(m => m.Expression).Returns(data.Expression);
            mockSet.As<IQueryable<EF_Person>>().Setup(m => m.ElementType).Returns(data.ElementType);
            mockSet.As<IQueryable<EF_Person>>().Setup(m => m.GetEnumerator()).Returns(data.GetEnumerator());

            var mockContext = new Mock<FakeDbContext>();
            mockContext.Setup(c => c.EF_Persons).Returns(mockSet.Object);

            var people = mockContext.Object.EF_Persons
                .Where(p => p.Age > 0)
                .SortByKey("age")
                .Take(50)
                .ToList();

            Assert.AreEqual(2, people[0].Age);
        }
    }
}