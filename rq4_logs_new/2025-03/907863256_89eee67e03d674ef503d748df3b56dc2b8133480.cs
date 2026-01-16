using CaptoneProject_IOTS_BOs.Constant;
using CaptoneProject_IOTS_BOs.DTO.PaginationDTO;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.RefundRequestController
{
    [Route("api/refund-request")]
    [Authorize]
    [ApiController]
    public class RefundRequestController : MyBaseController.MyBaseController
    {
        private readonly IRefundRequestService refundRequestService;

        public RefundRequestController(IRefundRequestService refundRequestService)
        {
            this.refundRequestService = refundRequestService;
        }

        [HttpPost("get-pagination")]
        public async Task<IActionResult> GetPagination(
            [FromQuery] RefundRequestStatusEnum? statusFilter,
            [FromBody] PaginationRequest payload
        )
        {
            var res = await refundRequestService.GetPaginationRefundRequest((int?)statusFilter, payload);

            return GetActionResult(res);
        }

        [HttpPost("handled/{refundRequestId}")]
        public async Task<IActionResult> UpdateStatusToHandled(int refundRequestId)
        {
            var res = await refundRequestService.UpdateStatusToHandled(refundRequestId);

            return GetActionResult(res);
        }
    }
}