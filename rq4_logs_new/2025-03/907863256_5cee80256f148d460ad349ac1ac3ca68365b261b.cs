using CaptoneProject_IOTS_BOs.Constant;
using CaptoneProject_IOTS_BOs.DTO.CashoutRequestDTO;
using CaptoneProject_IOTS_BOs.DTO.PaginationDTO;
using CaptoneProject_IOTS_BOs.DTO.UserRequestDTO;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Net.Http.Headers;

namespace CaptoneProject_IOTS_API.Controllers.CashoutRequestController
{
    [Route("api/cashout-request")]
    [ApiController]
    [Authorize]
    public class CashoutRequestController : MyBaseController.MyBaseController
    {
        private readonly ICashoutService cashoutService;

        public CashoutRequestController(ICashoutService cashoutService)
        {
            this.cashoutService = cashoutService;
        }

        [HttpPost("get-pagination")]
        public async Task<IActionResult> GetPagination(
            [FromQuery] CashoutRequestStatusEnum? statusFilter,
            [FromBody] PaginationRequest payload)
        {
            var res = await cashoutService.GetPaginationCashoutRequest((int?)statusFilter, payload);

            return GetActionResult(res);
        }

        [HttpPost("create")]
        public async Task<IActionResult> CreateCashoutRequest(
            [FromBody] CreateCashoutRequestDTO payload)
        {
            var res = await cashoutService.CreateCashoutRequest(payload);

            return GetActionResult(res);
        }

        [HttpPut("approve/{id}")]
        public async Task<IActionResult> CreateCashoutRequest(int id)
        {
            var res = await cashoutService.ApproveOrRejectCashoutRequest(id, true);

            return GetActionResult(res);
        }


        [HttpPut("reject/{id}")]
        public async Task<IActionResult> RejectCashoutRequest(
            int id, 
            [FromBody] RemarkDTO remarks)
        {
            var res = await cashoutService.ApproveOrRejectCashoutRequest(id, true, remarks);

            return GetActionResult(res);
        }
    }
}