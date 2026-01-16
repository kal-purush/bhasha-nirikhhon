using Com.Danliris.Service.Inventory.Lib.Models.GarmentLeftoverWarehouse.GarmentLeftoverWarehouseReceiptAvalModels;
using Com.Danliris.Service.Inventory.Lib.Services;
using Com.Danliris.Service.Inventory.Lib.Services.GarmentLeftoverWarehouse.GarmentLeftoverWarehouseReceiptAvalServices;
using Com.Danliris.Service.Inventory.Lib.ViewModels.GarmentLeftoverWarehouse.GarmentLeftoverWarehouseReceiptAvalViewModels;
using Com.Danliris.Service.Inventory.WebApi.Helpers;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Com.Danliris.Service.Inventory.WebApi.Controllers.v1.GarmentLeftoverWarehouse.GarmentLeftoverWarehouseReceiptAvalControllers
{
    [Produces("application/json")]
    [ApiVersion("1.0")]
    [Route("v{version:apiVersion}/garment/leftover-warehouse-receipts/aval")]
    [Authorize]
    public class GarmentLeftoverWarehouseReceiptAvalController : BaseController<GarmentLeftoverWarehouseReceiptAval, GarmentLeftoverWarehouseReceiptAvalViewModel, IGarmentLeftoverWarehouseReceiptAvalService>
    {
        public GarmentLeftoverWarehouseReceiptAvalController(IIdentityService identityService, IValidateService validateService, IGarmentLeftoverWarehouseReceiptAvalService service) : base(identityService, validateService, service, "1.0.0")
        {
        }
    }
}