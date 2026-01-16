using System;
using System.Linq;
using Catalog.DataAccessLayer;
using Catalog.DataAccessLayer.Helpers;
using Catalog.Domain.Entity;
using Catalog.Domain.Model;
using LinqKit;

namespace Catalog.BusinessLogicLayer.Service.Report
{
    public class StatisticsService : IStatisticsService
    {
        private readonly CatalogDbContext _dbContext;
        
        public StatisticsService(CatalogDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public MemberExchangeInformation GetExchangeInformation(Member member)
        {
            var memberExchangeInformation = new MemberExchangeInformation();
            
            _dbContext.Messages
                .AsExpandable()
                .WhereConsumerEquals(member, false)
                .WhereIsSucceeded(true)
                .Count();

            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _dbContext.Dispose();
        }
    }
}