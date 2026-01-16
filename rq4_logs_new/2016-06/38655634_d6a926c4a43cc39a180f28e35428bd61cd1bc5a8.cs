using System;
using System.Data.Entity;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Moq;

using Hatfield.EnviroData.Core;

namespace Hatfield.EnviroData.DataProfile.WQ.Test
{
    public class InMemoryDatabaseGenerator
    {
        private Mock<DbSet<T>> ToDbSet<T>(List<T> sourceList) where T : class
        {
            var queryable = sourceList.AsQueryable();

            var dbSet = new Mock<DbSet<T>>();
            dbSet.As<IQueryable<T>>().Setup(m => m.Provider).Returns(queryable.Provider);
            dbSet.As<IQueryable<T>>().Setup(m => m.Expression).Returns(queryable.Expression);
            dbSet.As<IQueryable<T>>().Setup(m => m.ElementType).Returns(queryable.ElementType);
            dbSet.As<IQueryable<T>>().Setup(m => m.GetEnumerator()).Returns(queryable.GetEnumerator());
            dbSet.Setup(d => d.Add(It.IsAny<T>())).Callback<T>(sourceList.Add);
            return dbSet;
        }


        private DbSet<Site> CreateMockSiteDb()
        {
            var data = new List<Site> { 
                new Site{
                    Latitude = 123.4,
                    Longitude = 234.5
                }
            };

            var mockSiteDbSet = ToDbSet<Site>(data);

            //add more custom query mock here

            return mockSiteDbSet.Object;
        
        }



        public DbContext CreateTestDbContext()
        {
            var mockDbContext = new Mock<DbContext>();

            mockDbContext.Setup(x => x.Set<Site>()).Returns(CreateMockSiteDb());
            return mockDbContext.Object;
        }
    }
}