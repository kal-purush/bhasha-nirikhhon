using CaptoneProject_IOTS_BOs.DTO.FeedbackDTO;
using CaptoneProject_IOTS_BOs.DTO.PaginationDTO;
using CaptoneProject_IOTS_BOs.DTO.ProductDTO;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.FeedbackController
{
    [Route("api/feedback")]
    [ApiController]
    public class FeedbackController : MyBaseController.MyBaseController
    {
        private readonly IFeedbackService feedbackService;

        public FeedbackController(IFeedbackService feedbackService)
        {
            this.feedbackService = feedbackService;
        }

        [HttpPost("product/get-pagination")]
        public async Task<IActionResult> GetProductFeedbackPagination(
            [FromBody] GenericPaginationRequest<ProductRequestDTO> payload    
        )
        {
            var res = await feedbackService.GetFeedbackPagination(payload.AdvancedFilter, payload.PaginationRequest);

            return GetActionResult(res);
        }

        [HttpPost("create")]
        [Authorize]
        public async Task<IActionResult> CreateOrderFeedback(
            [FromBody] StoreOrderFeedbackRequestDTO payload    
        )
        {
            var res = await feedbackService.CreateOrderFeedback(payload);

            return GetActionResult(res);
        }
    }
}