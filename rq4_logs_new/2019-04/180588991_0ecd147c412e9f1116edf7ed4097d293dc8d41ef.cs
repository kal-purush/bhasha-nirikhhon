using System;
using System.Linq;
using Catalog.BusinessLogicLayer.Service.Report;
using Catalog.BusinessLogicLayer.UnitTests.Providers;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Catalog.BusinessLogicLayer.UnitTests.Report
{
    public class StatisticsServiceTests
    {
        [Fact]
        public void GetExchangeInformation()
        {
            var dbContext         = DbContextProvider.RequireDbContext();
            var member            = dbContext.Members.Include(it => it.SubSystems).First();
            var statisticsService = new StatisticsService(dbContext);
            var memberExchangeInformation =
                statisticsService.GetExchangeInformation(member, DateTime.Now.AddDays(-10), DateTime.Now);

            Assert.True(true);
        }
    }
}