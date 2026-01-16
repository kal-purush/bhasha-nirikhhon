using Com.Danliris.Service.Finance.Accounting.Lib;
using Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.GarmentInvoicePurchasingDisposition;
using Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.Services.DailyBankTransaction;
using Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.Services.JournalTransaction;
using Com.Danliris.Service.Finance.Accounting.Lib.Models.GarmentInvoicePurchasingDisposition;
using Com.Danliris.Service.Finance.Accounting.Lib.Services.HttpClientService;
using Com.Danliris.Service.Finance.Accounting.Lib.Services.IdentityService;
using Com.Danliris.Service.Finance.Accounting.Test.Helpers;
using Com.Danliris.Service.Finance.Accounting.Test.Services.OthersExpenditureProofDocument.Helper;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Com.Danliris.Service.Finance.Accounting.Test.Services
{
    public class GarmentInvoicePurchasingDispositionServiceTest
    {
        private const string ENTITY = "GarmentPurchasingDispositionService";
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected string GetCurrentMethod()
        {

            var st = new StackTrace();
            var sf = st.GetFrame(1);

            return string.Concat(sf.GetMethod().Name, "_", ENTITY);
        }
        private FinanceDbContext GetDbContext(string testName)
        {
            var serviceProvider = new ServiceCollection()
              .AddEntityFrameworkInMemoryDatabase()
              .BuildServiceProvider();

            var optionsBuilder = new DbContextOptionsBuilder<FinanceDbContext>();
            optionsBuilder
                .UseInMemoryDatabase(testName)
                .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning))
                 .UseInternalServiceProvider(serviceProvider);

            var dbContext = new FinanceDbContext(optionsBuilder.Options);

            return dbContext;
        }

        private Mock<IServiceProvider> GetServiceProvider()
        {
            var serviceProvider = new Mock<IServiceProvider>();

            serviceProvider
                .Setup(x => x.GetService(typeof(IAutoJournalService)))
                .Returns(new AutoJournalServiceTestHelper());

            var mockAutoDailyBankTransaction = new Mock<IAutoDailyBankTransactionService>();
            mockAutoDailyBankTransaction
                .Setup(x => x.AutoCreateFromGarmentInvoicePurchasingDisposition(It.IsAny<GarmentInvoicePurchasingDispositionModel>()))
                .ReturnsAsync(1);
            serviceProvider
                .Setup(x => x.GetService(typeof(IAutoDailyBankTransactionService)))
                .Returns(mockAutoDailyBankTransaction.Object);

            var documentNoContentResult = new BaseResponse<string>();

            var mockHttpClient = new Mock<IHttpClientService>();
            mockHttpClient
                .Setup(http => http.GetAsync(It.IsAny<string>()))
                .ReturnsAsync(new HttpResponseMessage() { Content = new StringContent(JsonConvert.SerializeObject(documentNoContentResult)) });

            serviceProvider
                .Setup(x => x.GetService(typeof(IHttpClientService)))
                .Returns(mockHttpClient.Object);

            serviceProvider
                .Setup(x => x.GetService(typeof(IIdentityService)))
                .Returns(new IdentityService() { Token = "Token", Username = "Test", TimezoneOffset = 7 });

            return serviceProvider;
        }

        [Fact]
        public async Task Should_Success_Create()
        {
            var serviceProviderMock = GetServiceProvider();
            var dbContext = GetDbContext(GetCurrentMethod());

            var service = new GarmentInvoicePurchasingDispositionService(serviceProviderMock.Object, dbContext);

            var result = await service.CreateAsync(new GarmentInvoicePurchasingDispositionModel() { Items = new List<GarmentInvoicePurchasingDispositionItemModel>() });
            Assert.NotEqual(0, result);
        }

    }
}