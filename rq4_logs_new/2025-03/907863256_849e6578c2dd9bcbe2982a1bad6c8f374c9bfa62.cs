using CaptoneProject_IOTS_BOs.DTO.PaginationDTO;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using static CaptoneProject_IOTS_BOs.Constant.ProductConst;

namespace CaptoneProject_IOTS_API.Controllers.FeedbackController
{
    [Route("api/rating")]
    [ApiController]
    public class RatingController : MyBaseController.MyBaseController
    {
        private readonly IRatingService ratingService;
        public RatingController(IRatingService ratingService)
        {
            this.ratingService = ratingService;
        }

        [HttpPost]
        [Route("feedback/get-pagination/{productId}")]
        public async Task<IActionResult> GetFeedbackPagination(
            int productId,
            [FromQuery] ProductTypeEnum productType,
            [FromBody] PaginationRequest payload
        )
        {
            var res = await ratingService.GetFeedbackPagination(productId, productType, payload);

            return GetActionResult(res);
        }
    }
}