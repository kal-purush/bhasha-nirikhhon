using CaptoneProject_IOTS_BOs.Constant;
using CaptoneProject_IOTS_BOs.DTO.PaginationDTO;
using CaptoneProject_IOTS_BOs.DTO.UserRequestDTO;
using CaptoneProject_IOTS_BOs.DTO.WarrantyRequestDTO;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.WarrantyRequestController
{
    [Route("api/warranty-request")]
    [ApiController]
    [Authorize]
    public class WarrantyRequestController : MyBaseController.MyBaseController
    {
        private readonly IWarrantyRequestService warrantyRequestService;

        public WarrantyRequestController(IWarrantyRequestService warrantyRequestService)
        {
            this.warrantyRequestService = warrantyRequestService;
        }

        [HttpGet("get/{id}")]
        [Authorize]
        public async Task<IActionResult> GetWarrantyRequestById(int id)
        {
            var res = await warrantyRequestService.GetWarrantyRequestById(id);

            return GetActionResult(res);
        }

        [HttpPost("create")]
        public async Task<IActionResult> CreateWarrantyRequest(
            [FromBody] WarrantyRequestRequestDTO payload)
        {
            var res = await warrantyRequestService.CreateCustomerWarrantyRequest(payload);

            return GetActionResult(res);
        }

        [HttpPost("get-pagination")]
        public async Task<IActionResult> GetPaginationWarrantyRequest(
            [FromQuery] WarrantyRequestStatusEnum? statusFilter,
            [FromBody] PaginationRequest payload)
        {
            var res = await warrantyRequestService.GetWarrantyRequestPagination(statusFilter, payload);

            return GetActionResult(res);
        }

        [HttpPost("store/reject/{id}")]
        public async Task<IActionResult> RejectWarrantyRequest(int id,
            [FromBody] RemarkDTO payload)
        {
            var res = await warrantyRequestService.StoreApproveOrRejectWarrantyRequest(id, false, payload);

            return GetActionResult(res);
        }

        [HttpPost("store/approve/{id}")]
        public async Task<IActionResult> ApproveWarrantyRequest(int id)
        {
            var res = await warrantyRequestService.StoreApproveOrRejectWarrantyRequest(id, true);

            return GetActionResult(res);
        }

        [HttpPost("confirm-success/{id}")]
        public async Task<IActionResult> CustomerConfirmSuccess(int id)
        {
            var res = await warrantyRequestService.ConfirmSuccess(id);

            return GetActionResult(res);
        }
    }
}