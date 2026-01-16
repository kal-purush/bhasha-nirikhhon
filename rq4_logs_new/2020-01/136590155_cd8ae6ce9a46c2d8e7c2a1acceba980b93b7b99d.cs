using Com.Danliris.Sales.Test.BussinesLogic.DataUtils.Garment.GarmentMerchandiser;
using Com.Danliris.Sales.Test.BussinesLogic.DataUtils.GarmentPreSalesContractDataUtils;
using Com.Danliris.Sales.Test.BussinesLogic.Utils;
using Com.Danliris.Service.Sales.Lib;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.CostCalculationGarments;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.GarmentPreSalesContractFacades;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Interface;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Interface.CostCalculationGarmentLogic;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Logic.CostCalculationGarments;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Logic.GarmentMasterPlan.MonitoringLogics;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Logic.GarmentPreSalesContractLogics;
using Com.Danliris.Service.Sales.Lib.Models.CostCalculationGarments;
using Com.Danliris.Service.Sales.Lib.Services;
using Microsoft.AspNetCore.JsonPatch;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Com.Danliris.Sales.Test.BussinesLogic.Facades.Garment.GarmentMerchandiser
{
    public class ProfitGarmentBySectionReportTest
    {
        private const string ENTITY = "CostCalculationBySectionReport";

        [MethodImpl(MethodImplOptions.NoInlining)]
        private string GetCurrentMethod()
        {
            StackTrace st = new StackTrace();
            StackFrame sf = st.GetFrame(1);

            return string.Concat(sf.GetMethod().Name, "_", ENTITY);
        }

        private SalesDbContext DbContext(string testName)
        {
            DbContextOptionsBuilder<SalesDbContext> optionsBuilder = new DbContextOptionsBuilder<SalesDbContext>();
            optionsBuilder
                .UseInMemoryDatabase(testName)
                .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning));

            SalesDbContext dbContext = new SalesDbContext(optionsBuilder.Options);

            return dbContext;
        }

        protected virtual Mock<IServiceProvider> GetServiceProviderMock(SalesDbContext dbContext)
        {
            var serviceProviderMock = new Mock<IServiceProvider>();

            IIdentityService identityService = new IdentityService { Username = "Username" };

            CostCalculationGarmentMaterialLogic costCalculationGarmentMaterialLogic = new CostCalculationGarmentMaterialLogic(serviceProviderMock.Object, identityService, dbContext);
            CostCalculationGarmentLogic costCalculationGarmentLogic = new CostCalculationGarmentLogic(costCalculationGarmentMaterialLogic, serviceProviderMock.Object, identityService, dbContext);
            ProfitGarmentBySectionReportLogic profitgarmentBySectionReportLogic = new ProfitGarmentBySectionReportLogic(identityService, dbContext);

            GarmentPreSalesContractLogic garmentPreSalesContractLogic = new GarmentPreSalesContractLogic(identityService, dbContext);

            var azureImageFacadeMock = new Mock<IAzureImageFacade>();
            azureImageFacadeMock
                .Setup(s => s.DownloadImage(It.IsAny<string>(), It.IsAny<string>()))
                .ReturnsAsync("");

            serviceProviderMock
                .Setup(x => x.GetService(typeof(IdentityService)))
                .Returns(identityService);
            serviceProviderMock
                .Setup(x => x.GetService(typeof(CostCalculationGarmentLogic)))
                .Returns(costCalculationGarmentLogic);
            serviceProviderMock
                .Setup(x => x.GetService(typeof(GarmentPreSalesContractLogic)))
                .Returns(garmentPreSalesContractLogic);

            serviceProviderMock
                .Setup(x => x.GetService(typeof(ProfitGarmentBySectionReportLogic)))
                .Returns(profitgarmentBySectionReportLogic);

            serviceProviderMock
                .Setup(x => x.GetService(typeof(IAzureImageFacade)))
                .Returns(azureImageFacadeMock.Object);

            return serviceProviderMock;
        }

        protected CostCalculationGarmentDataUtil DataUtil(CostCalculationGarmentFacade facade, SalesDbContext dbContext = null)
        {
            var serviceProvider = GetServiceProviderMock(dbContext).Object;

            GarmentPreSalesContractFacade garmentPreSalesContractFacade = new GarmentPreSalesContractFacade(serviceProvider, dbContext);
            GarmentPreSalesContractDataUtil garmentPreSalesContractDataUtil = new GarmentPreSalesContractDataUtil(garmentPreSalesContractFacade);

            CostCalculationGarmentDataUtil costCalculationGarmentDataUtil = new CostCalculationGarmentDataUtil(facade, garmentPreSalesContractDataUtil);

            return costCalculationGarmentDataUtil;
        }

        [Fact]
        public async void Get_Report_Success()
        {
            var dbContext = DbContext(GetCurrentMethod());
            var serviceProvider = GetServiceProviderMock(dbContext).Object;

            CostCalculationGarmentFacade facade = new CostCalculationGarmentFacade(serviceProvider, dbContext);

            var data = await DataUtil(facade, dbContext).GetTestData();

            IProfitGarmentBySectionReport profitgarmentBySectionReportLogic = new ProfitGarmentBySectionReportFacade(serviceProvider, dbContext);

            var filter = new
            {
                section = data.Section,
                dateFrom = data.DeliveryDate,
                dateTo = data.DeliveryDate,
            };

            var Response = profitgarmentBySectionReportLogic.Read(filter: JsonConvert.SerializeObject(filter));

            Assert.NotEqual(Response.Item2, 0);
        }

        [Fact]
        public async void Get_Report_Error()
        {
            var dbContext = DbContext(GetCurrentMethod());
            var serviceProvider = GetServiceProviderMock(dbContext).Object;

            CostCalculationGarmentFacade facade = new CostCalculationGarmentFacade(serviceProvider, dbContext);

            var data = await DataUtil(facade, dbContext).GetTestData();

            IProfitGarmentBySectionReport profitgarmentBySectionReportLogic = new ProfitGarmentBySectionReportFacade(serviceProvider, dbContext);

            var filter = new
            {
                section = data.Section,
                dateFrom = data.DeliveryDate.AddDays(30),
                dateTo = data.DeliveryDate.AddDays(30),
            };

            var Response = profitgarmentBySectionReportLogic.Read(filter: JsonConvert.SerializeObject(filter));

            Assert.Equal(Response.Item2, 0);
        }

        [Fact]
        public async void Get_Excel_Success()
        {
            var dbContext = DbContext(GetCurrentMethod());
            var serviceProvider = GetServiceProviderMock(dbContext).Object;

            CostCalculationGarmentFacade facade = new CostCalculationGarmentFacade(serviceProvider, dbContext);

            var data = await DataUtil(facade, dbContext).GetTestData();

            IProfitGarmentBySectionReport profitgarmentBySectionReportLogic = new ProfitGarmentBySectionReportFacade(serviceProvider, dbContext);

            var filter = new
            {
                section = data.Section,
                dateFrom = data.DeliveryDate,
                dateTo = data.DeliveryDate,
            };

            var Response = profitgarmentBySectionReportLogic.GenerateExcel(filter: JsonConvert.SerializeObject(filter));

            Assert.NotNull(Response.Item2);
        }
        [Fact]
        public async void Get_Excel_Error()
        {
            var dbContext = DbContext(GetCurrentMethod());
            var serviceProvider = GetServiceProviderMock(dbContext).Object;

            CostCalculationGarmentFacade facade = new CostCalculationGarmentFacade(serviceProvider, dbContext);

            var data = await DataUtil(facade, dbContext).GetTestData();

            IProfitGarmentBySectionReport profitgarmentBySectionReportLogic = new ProfitGarmentBySectionReportFacade(serviceProvider, dbContext);

            var filter = new
            {
                section = data.Section,
                dateFrom = data.DeliveryDate.AddDays(30),
                dateTo = data.DeliveryDate.AddDays(30)
            };

            var Response = profitgarmentBySectionReportLogic.GenerateExcel(filter: JsonConvert.SerializeObject(filter));

            Assert.NotNull(Response.Item2);
        }
    }
}