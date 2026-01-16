using Com.Danliris.Service.Sales.Lib.BusinessLogic.Interface.ProductionOrder;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Logic.FinishingPrinting;
using Com.Danliris.Service.Sales.Lib.Models.ProductionOrder;
using Com.Danliris.Service.Sales.Lib.Services;
using Com.Danliris.Service.Sales.Lib.Utilities;
using Com.Danliris.Service.Sales.Lib.ViewModels.Report;
using Com.Danliris.Service.Sales.Lib.ViewModels.Report.OrderStatusReport;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.ProductionOrder
{
    public class ShinProductionOrderFacade : IShinProductionOrder
    {
        private readonly SalesDbContext DbContext;
        private readonly DbSet<ProductionOrderModel> DbSet;
        private readonly IIdentityService identityService;
        private readonly ProductionOrderLogic productionOrderLogic;
        private readonly ShinFinishingPrintingSalesContractLogic finishingPrintingSalesContractLogic;
        private readonly IServiceProvider _serviceProvider;
        public Task<int> CreateAsync(ProductionOrderModel model)
        {
            throw new NotImplementedException();
        }

        public Task<int> DeleteAsync(int id)
        {
            throw new NotImplementedException();
        }

        public Task<MemoryStream> GenerateExcel(string salesContractNo, string orderNo, string orderTypeId, string processTypeId, string buyerId, string accountId, DateTime? dateFrom, DateTime? dateTo, int offset)
        {
            throw new NotImplementedException();
        }

        public Task<ProductionOrderReportDetailViewModel> GetDetailReport(long no)
        {
            throw new NotImplementedException();
        }

        public List<MonthlyOrderQuantity> GetMonthlyOrderIdsByOrderType(int year, int month, int orderTypeId, int timeoffset)
        {
            throw new NotImplementedException();
        }

        public List<YearlyOrderQuantity> GetMonthlyOrderQuantityByYearAndOrderType(int year, int orderTypeId, int timeoffset)
        {
            throw new NotImplementedException();
        }

        public Task<Tuple<List<ProductionOrderReportViewModel>, int>> GetReport(string salesContractNo, string orderNo, string orderTypeId, string processTypeId, string buyerId, string accountId, DateTime? dateFrom, DateTime? dateTo, int page, int size, string Order, int offset)
        {
            throw new NotImplementedException();
        }

        public ReadResponse<ProductionOrderModel> Read(int page, int size, string order, List<string> select, string keyword, string filter)
        {
            throw new NotImplementedException();
        }

        public Task<ProductionOrderModel> ReadByIdAsync(int id)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateAsync(int id, ProductionOrderModel model)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateDistributedQuantity(List<int> id, List<double> distributedQuantity)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateIsCalculated(int id, bool flag)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateIsCompletedFalse(int id)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateIsCompletedTrue(int id)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateRequestedFalse(List<int> ids)
        {
            throw new NotImplementedException();
        }

        public Task<int> UpdateRequestedTrue(List<int> ids)
        {
            throw new NotImplementedException();
        }
    }
}