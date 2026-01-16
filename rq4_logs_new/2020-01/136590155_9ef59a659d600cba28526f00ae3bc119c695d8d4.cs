using AutoMapper;
using Com.Danliris.Sales.Test.WebApi.Utils;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Interface.FinishingPrinting;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Interface.FinishingPrintingCostCalculation;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Interface.ProductionOrder;
using Com.Danliris.Service.Sales.Lib.Models.FinishingPrinting;
using Com.Danliris.Service.Sales.Lib.Models.FinishingPrintingCostCalculation;
using Com.Danliris.Service.Sales.Lib.Models.ProductionOrder;
using Com.Danliris.Service.Sales.Lib.Services;
using Com.Danliris.Service.Sales.Lib.ViewModels.FinishingPrinting;
using Com.Danliris.Service.Sales.Lib.ViewModels.FinishingPrintingCostCalculation;
using Com.Danliris.Service.Sales.Lib.ViewModels.IntegrationViewModel;
using Com.Danliris.Service.Sales.Lib.ViewModels.ProductionOrder;
using Com.Danliris.Service.Sales.Lib.ViewModels.Report.OrderStatusReport;
using Com.Danliris.Service.Sales.WebApi.Controllers;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using static Com.Danliris.Service.Sales.WebApi.Controllers.ShinProductionOrderController;

namespace Com.Danliris.Sales.Test.WebApi.Controllers
{
    public class ShinProductionOrderControllerTest : BaseControllerTest<ShinProductionOrderController, ProductionOrderModel, ShinProductionOrderViewModel, IShinProductionOrder>
    {

        protected override ProductionOrderModel Model => new ProductionOrderModel()
        {
            Details = new List<ProductionOrder_DetailModel>(),
            LampStandards = new List<ProductionOrder_LampStandardModel>(),
            RunWidths = new List<ProductionOrder_RunWidthModel>()
        };

        protected override ShinProductionOrderViewModel ViewModel => new ShinProductionOrderViewModel()
        {
            FinishingPrintingSalesContract = new ShinFinishingPrintingSalesContractViewModel()
            {
                Id = 1
            },
            Details = new List<ProductionOrder_DetailViewModel>(),
            RunWidth = new List<ProductionOrder_RunWidthViewModel>(),
            LampStandards = new List<ProductionOrder_LampStandardViewModel>()
        };

        protected override List<ShinProductionOrderViewModel> ViewModels => new List<ShinProductionOrderViewModel>() { ViewModel };

        protected override ShinProductionOrderController GetController((Mock<IIdentityService> IdentityService, Mock<IValidateService> ValidateService, Mock<IShinProductionOrder> Facade, Mock<IMapper> Mapper, Mock<IServiceProvider> ServiceProvider) mocks)
        {
            var user = new Mock<ClaimsPrincipal>();
            var claims = new Claim[]
            {
                new Claim("username", "unittestusername")
            };
            user.Setup(u => u.Claims).Returns(claims);
            Mock<IFinishingPrintingPreSalesContractFacade> preSCFacade = new Mock<IFinishingPrintingPreSalesContractFacade>();
            Mock<IFinishingPrintingCostCalculationService> ccFacade = new Mock<IFinishingPrintingCostCalculationService>();
            Mock<IShinFinishingPrintingSalesContractFacade> scFacade = new Mock<IShinFinishingPrintingSalesContractFacade>();
            preSCFacade.Setup(s => s.ReadByIdAsync(It.IsAny<int>())).ReturnsAsync(new FinishingPrintingPreSalesContractModel());
            ccFacade.Setup(s => s.ReadParent(It.IsAny<long>())).ReturnsAsync(new FinishingPrintingCostCalculationModel());
            scFacade.Setup(s => s.ReadParent(It.IsAny<long>())).ReturnsAsync(new FinishingPrintingSalesContractModel());
            mocks.ServiceProvider.Setup(s => s.GetService(typeof(IHttpClientService))).Returns(new HttpClientTestService());
            mocks.ServiceProvider.Setup(s => s.GetService(typeof(IFinishingPrintingCostCalculationService))).Returns(ccFacade.Object);
            mocks.ServiceProvider.Setup(s => s.GetService(typeof(IFinishingPrintingPreSalesContractFacade))).Returns(preSCFacade.Object);
            mocks.ServiceProvider.Setup(s => s.GetService(typeof(IShinFinishingPrintingSalesContractFacade))).Returns(scFacade.Object);
            mocks.Mapper.Setup(x => x.Map<FinishingPrintingCostCalculationViewModel>(It.IsAny<FinishingPrintingCostCalculationModel>())).Returns(new FinishingPrintingCostCalculationViewModel() { PreSalesContract = new FinishingPrintingPreSalesContractViewModel() { Id = 1 } });
            mocks.Mapper.Setup(x => x.Map<FinishingPrintingPreSalesContractViewModel>(It.IsAny<FinishingPrintingPreSalesContractModel>())).Returns(new FinishingPrintingPreSalesContractViewModel());
            mocks.Mapper.Setup(s => s.Map<ShinFinishingPrintingSalesContractViewModel>(It.IsAny<FinishingPrintingSalesContractModel>())).Returns(new ShinFinishingPrintingSalesContractViewModel() { CostCalculation = new FinishingPrintingCostCalculationViewModel() { Id = 1 } });
            ShinProductionOrderController controller = (ShinProductionOrderController)Activator.CreateInstance(typeof(ShinProductionOrderController), mocks.IdentityService.Object, mocks.ValidateService.Object, mocks.Facade.Object, mocks.Mapper.Object, mocks.ServiceProvider.Object);
            controller.ControllerContext = new ControllerContext()
            {
                HttpContext = new DefaultHttpContext()
                {
                    User = user.Object
                }
            };
            controller.ControllerContext.HttpContext.Request.Headers["Authorization"] = "Bearer unittesttoken";
            controller.ControllerContext.HttpContext.Request.Path = new PathString("/v1/unit-test");
            return controller;
        }

        [Fact]
        public void Get_PDF_Success()
        {
            var vm = new ShinProductionOrderViewModel()
            {
                DistributedQuantity = 1,
                OrderQuantity = 1,
                FinishingPrintingSalesContract = new ShinFinishingPrintingSalesContractViewModel(),
                Run = "ru",
                RunWidth = new List<ProductionOrder_RunWidthViewModel>()
                {
                    new ProductionOrder_RunWidthViewModel()
                    {
                        Value = 1
                    },
                    new ProductionOrder_RunWidthViewModel()
                    {
                        Value = 2
                    },
                    new ProductionOrder_RunWidthViewModel()
                    {
                        Value = 3
                    }
                },
                StandardTests = new StandardTestsViewModel(),
                DeliveryDate = DateTimeOffset.UtcNow,
                Account = new AccountViewModel(),
                LampStandards = new List<ProductionOrder_LampStandardViewModel>()
                {
                    new ProductionOrder_LampStandardViewModel()
                },
                Details = new List<ProductionOrder_DetailViewModel>()
                {
                    new ProductionOrder_DetailViewModel()
                    {
                        ColorType = new ColorTypeViewModel(),
                        Quantity = 1,
                        Uom = new UomViewModel()
                    }
                },
                ArticleFabricEdge = "e",
                HandlingStandard = "e",
                MaterialOrigin = "a",
                Remark = "ee",
                ShrinkageStandard = "a"
            };
            var mocks = GetMocks();
            mocks.Facade.Setup(x => x.ReadByIdAsync(It.IsAny<int>())).ReturnsAsync(Model);
            mocks.Mapper.Setup(s => s.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>()))
                .Returns(vm);
            var controller = GetController(mocks);
            var response = controller.GetPDF(1).Result;

            Assert.NotNull(response);

        }

        [Fact]
        public void Get_PDF_NotFound()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(x => x.ReadByIdAsync(It.IsAny<int>())).ReturnsAsync(default(ProductionOrderModel));
            var controller = GetController(mocks);
            var response = controller.GetPDF(1).Result;

            int statusCode = this.GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.NotFound, statusCode);

        }

        [Fact]
        public void Get_PDF_Exception()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(x => x.ReadByIdAsync(It.IsAny<int>())).ThrowsAsync(new Exception("error"));
            var controller = GetController(mocks);
            var response = controller.GetPDF(1).Result;

            int statusCode = this.GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);

        }


        [Fact]
        public async Task Put_IsRequested_True_Success()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateRequestedTrue(It.IsAny<List<int>>())).ReturnsAsync(1);
            List<int> ids = new List<int>((int)viewModel.Id);
            var controller = GetController(mocks);
            var response = await controller.PutRequestedTrue(ids);

            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.NoContent, statusCode);
        }

        [Fact]
        public async Task Put_IsRequested_True_Null_BadRequest()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateRequestedTrue(It.IsAny<List<int>>())).ReturnsAsync(1);
            List<int> ids = new List<int>((int)viewModel.Id);
            var controller = GetController(mocks);
            var response = await controller.PutRequestedTrue(null);

            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.BadRequest, statusCode);
        }

        [Fact]
        public async Task Put_IsRequested_True_Exception_InternalServerError()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateRequestedTrue(It.IsAny<List<int>>())).ThrowsAsync(new Exception());
            List<int> ids = new List<int>((int)viewModel.Id);
            var controller = GetController(mocks);
            var response = await controller.PutRequestedTrue(ids);

            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public async Task Put_IsRequested_False_Success()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateRequestedFalse(It.IsAny<List<int>>())).ReturnsAsync(1);
            List<int> ids = new List<int>((int)viewModel.Id);
            var controller = GetController(mocks);
            var response = await controller.PutRequestedFalse(ids);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.NoContent, statusCode);
        }

        [Fact]
        public async Task Put_IsRequested_False_Null_BadRequest()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateRequestedFalse(It.IsAny<List<int>>())).ReturnsAsync(1);
            List<int> ids = new List<int>((int)viewModel.Id);
            var controller = GetController(mocks);
            var response = await controller.PutRequestedFalse(null);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.BadRequest, statusCode);
        }

        [Fact]
        public async Task Put_IsRequested_False_Exception_InternalServer()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateRequestedFalse(It.IsAny<List<int>>())).ThrowsAsync(new Exception());
            List<int> ids = new List<int>((int)viewModel.Id);
            var controller = GetController(mocks);
            var response = await controller.PutRequestedFalse(ids);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public async Task Put_IsCompleted_True_Success()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateIsCompletedTrue(It.IsAny<int>())).ReturnsAsync(1);
            var controller = GetController(mocks);
            var response = await controller.PutIsCompletedTrue((int)viewModel.Id);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.NoContent, statusCode);
        }

        [Fact]
        public async Task Put_IsCompleted_True_BadRequest()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateIsCompletedTrue(It.IsAny<int>())).ReturnsAsync(1);
            var controller = GetController(mocks);
            var response = await controller.PutIsCompletedTrue(0);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.BadRequest, statusCode);
        }

        [Fact]
        public async Task Put_IsCompleted_True_InternalServer()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateIsCompletedTrue(It.IsAny<int>())).ThrowsAsync(new Exception());
            var controller = GetController(mocks);
            var response = await controller.PutIsCompletedTrue((int)viewModel.Id);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public async Task Put_IsCompleted_False_Success()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateIsCompletedFalse(It.IsAny<int>())).ReturnsAsync(1);
            var controller = GetController(mocks);
            var response = await controller.PutIsCompletedFalse((int)viewModel.Id);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.NoContent, statusCode);
        }

        [Fact]
        public async Task Put_IsCompleted_False_BadRequest()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateIsCompletedFalse(It.IsAny<int>())).ReturnsAsync(1);
            var controller = GetController(mocks);
            var response = await controller.PutIsCompletedFalse(0);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.BadRequest, statusCode);
        }

        [Fact]
        public async Task Put_IsCompleted_False_InternalServer()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateIsCompletedFalse(It.IsAny<int>())).ThrowsAsync(new Exception());
            var controller = GetController(mocks);
            var response = await controller.PutIsCompletedFalse((int)viewModel.Id);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public async Task Put_Distributed_Quantity_Success()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var distributedQty = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id,
                DistributedQuantity = distributedQty
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateDistributedQuantity(It.IsAny<List<int>>(), It.IsAny<List<double>>())).ReturnsAsync(1);
            List<SppParams> data = new List<SppParams>()
            {
                new SppParams()
                {
                    id = "1",
                    context= "cpmt",
                    distributedQuantity = 1
                }
            };
            List<int> ids = new List<int>();
            List<double> distributedQuantity = new List<double>();
            foreach (var item in data)
            {
                ids.Add(int.Parse(item.id));
                distributedQuantity.Add((double)item.distributedQuantity);
            };

            var controller = GetController(mocks);
            var response = await controller.PutDistributedQuantity(data);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.NoContent, statusCode);
        }

        [Fact]
        public async Task Put_Distributed_Quantity_BadRequest()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var distributedQty = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id,
                DistributedQuantity = distributedQty
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateDistributedQuantity(It.IsAny<List<int>>(), It.IsAny<List<double>>())).ReturnsAsync(1);
            List<SppParams> data = new List<SppParams>();
            List<int> ids = new List<int>();
            List<double> distributedQuantity = new List<double>();
            foreach (var item in data)
            {
                ids.Add(int.Parse(item.id));
                distributedQuantity.Add((double)item.distributedQuantity);
            };

            var controller = GetController(mocks);
            var response = await controller.PutDistributedQuantity(null);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.BadRequest, statusCode);
        }

        [Fact]
        public async Task Put_Distributed_Quantity_InternalServer()
        {
            var mocks = GetMocks();
            mocks.ValidateService.Setup(vs => vs.Validate(It.IsAny<ShinProductionOrderViewModel>())).Verifiable();
            var id = 1;
            var distributedQty = 1;
            var viewModel = new ShinProductionOrderViewModel()
            {
                Id = id,
                DistributedQuantity = distributedQty
            };
            mocks.Mapper.Setup(m => m.Map<ShinProductionOrderViewModel>(It.IsAny<ProductionOrderModel>())).Returns(viewModel);
            mocks.Facade.Setup(f => f.UpdateDistributedQuantity(It.IsAny<List<int>>(), It.IsAny<List<double>>())).ThrowsAsync(new Exception());
            List<SppParams> data = new List<SppParams>()
            {
                new SppParams()
                {
                    id = "1",
                    context= "cpmt",
                    distributedQuantity = 1
                }
            };
            List<int> ids = new List<int>();
            List<double> distributedQuantity = new List<double>();
            foreach (var item in data)
            {
                ids.Add(int.Parse(item.id));
                distributedQuantity.Add((double)item.distributedQuantity);
            };

            var controller = GetController(mocks);
            var response = await controller.PutDistributedQuantity(data);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public void Should_ReturnOK_GetMonthlySummaryByYearAndOrderType_Success()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(x => x.GetMonthlyOrderQuantityByYearAndOrderType(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>())).Returns(new List<YearlyOrderQuantity>());
            var controller = GetController(mocks);
            var response = controller.GetMonthlySummaryByYearAndOrderType(0, 1);

            int statusCode = this.GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.OK, statusCode);
        }

        [Fact]
        public void Should_ReturnFailed_GetMonthlySummaryByYearAndOrderType_ThrowException()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(x => x.GetMonthlyOrderQuantityByYearAndOrderType(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>())).Throws(new Exception());
            var controller = GetController(mocks);
            var response = controller.GetMonthlySummaryByYearAndOrderType(1, 1);

            int statusCode = this.GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public void Should_ReturnOK_GetMonthlyOrderIdsByOrderTypeId_Success()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(x => x.GetMonthlyOrderIdsByOrderType(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>())).Returns(new List<MonthlyOrderQuantity>());
            var controller = GetController(mocks);
            var response = controller.GetMonthlyOrderIdsByOrderTypeId(0, 0, 1);

            int statusCode = this.GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.OK, statusCode);
        }

        [Fact]
        public void Should_ReturnFailed_GetMonthlyOrderIdsByOrderTypeId_ThrowException()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(x => x.GetMonthlyOrderIdsByOrderType(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>())).Throws(new Exception());
            var controller = GetController(mocks);
            var response = controller.GetMonthlyOrderIdsByOrderTypeId(1, 1, 1);

            int statusCode = this.GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public async Task Put_IsCalculated_Success()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(f => f.UpdateIsCalculated(It.IsAny<int>(), It.IsAny<bool>())).ReturnsAsync(1);
            var controller = GetController(mocks);
            var response = await controller.PutIsCalculated(1, true);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.NoContent, statusCode);
        }

        [Fact]
        public async Task Put_IsCalculated_Exception()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(f => f.UpdateIsCalculated(It.IsAny<int>(), It.IsAny<bool>())).ThrowsAsync(new Exception());
            var controller = GetController(mocks);
            var response = await controller.PutIsCalculated(1, true);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.InternalServerError, statusCode);
        }

        [Fact]
        public async Task Put_IsCalculated_ID_0()
        {
            var mocks = GetMocks();
            mocks.Facade.Setup(f => f.UpdateIsCalculated(It.IsAny<int>(), It.IsAny<bool>())).ThrowsAsync(new Exception());
            var controller = GetController(mocks);
            var response = await controller.PutIsCalculated(0, true);
            int statusCode = GetStatusCode(response);
            Assert.Equal((int)HttpStatusCode.BadRequest, statusCode);
        }
        [Fact]
        public void Validate_ViewModel()
        {
            List<ShinProductionOrderViewModel> viewModels = new List<ShinProductionOrderViewModel>
            {
                new ShinProductionOrderViewModel{
                    Code = "ABC",
                    OrderQuantity = 5.48,
                    LampStandards = new List<ProductionOrder_LampStandardViewModel>{
                        new ProductionOrder_LampStandardViewModel{
                            Name = "Lampu",
                            Description = "Lampu Luar"
                        }
                    },
                    Details = new List<ProductionOrder_DetailViewModel>{
                        new ProductionOrder_DetailViewModel{
                            ColorRequest = "A",
                            Quantity = 5.4,
                            Uom = new UomViewModel{
                                Id = 1,
                                Unit = "MTR"
                            }
                        }
                    }
                }
            };
            foreach (var viewModel in viewModels)
            {
                var defaultValidationResult = viewModel.Validate(null);
                Assert.True(defaultValidationResult.Count() > 0);
            }
        }
    }
}