using Com.Danliris.Service.Sales.Lib;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Logic.DOReturn;
using Com.Danliris.Service.Sales.Lib.Models.DOReturn;
using Com.Danliris.Service.Sales.Lib.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Moq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Xunit;

namespace Com.Danliris.Sales.Test.BussinesLogic.Logic.DOReturn
{
 public   class DOReturnDetailItemLogicTest
    {
        private const string ENTITY = "DOReturnDetailItem";
        [MethodImpl(MethodImplOptions.NoInlining)]
        public string GetCurrentMethod()
        {
            StackTrace st = new StackTrace();
            StackFrame sf = st.GetFrame(1);

            return string.Concat(sf.GetMethod().Name, "_", ENTITY);
        }

        private SalesDbContext _dbContext(string testName)
        {
            DbContextOptionsBuilder<SalesDbContext> optionsBuilder = new DbContextOptionsBuilder<SalesDbContext>();
            optionsBuilder
                .UseInMemoryDatabase(testName)
                .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning));

            SalesDbContext dbContext = new SalesDbContext(optionsBuilder.Options);

            return dbContext;
        }

        public Mock<IServiceProvider> GetServiceProvider(string testname)
        {
            IIdentityService identityService = new IdentityService { Username = "Username" };
            var serviceProvider = new Mock<IServiceProvider>();

            serviceProvider
                .Setup(x => x.GetService(typeof(IdentityService)))
                .Returns(identityService);

            serviceProvider.Setup(s => s.GetService(typeof(SalesDbContext)))
                .Returns(_dbContext(testname));

            return serviceProvider;
        }

        [Fact]
        public void Read_Return_Success()
        {
            string testName = GetCurrentMethod();
            var dbContext = _dbContext(testName);
            IIdentityService identityService = new IdentityService { Username = "Username" };

            dbContext.DOReturnDetailItems.Add(new DOReturnDetailItemModel()
            {
                Active = true,
                CreatedAgent = "",
                CreatedBy = "someone",
                CreatedUtc = DateTime.UtcNow,
                DeletedAgent = "",
                DeletedBy = "",
                DeletedUtc = DateTime.UtcNow,
               
                IsDeleted = false,
                LastModifiedAgent = "LastModifiedAgent"

            });
            dbContext.SaveChanges();
            DOReturnDetailItemLogic unitUnderTest = new DOReturnDetailItemLogic(GetServiceProvider(testName).Object, identityService, dbContext);

            dbContext.SaveChanges();
            int page = 1;
            int size = 1;
            string order = "{}";
            string keyword = null;
            string filter = @"{""ProductName"":""""}";

            unitUnderTest.Read(page, size, order, new List<string>() { "" }, keyword, "{}");

        }


    }
}