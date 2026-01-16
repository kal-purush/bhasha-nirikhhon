using AutoMapper;
using iPhoneBE.Data.Entities;
using iPhoneBE.Data.Models.ReviewModel;
using iPhoneBE.Data.ViewModels.ReviewDTO;
using iPhoneBE.Service.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace iPhoneBE.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReviewController : ControllerBase
    {
        private readonly IReviewServices _reviewServices;
        private readonly IMapper _mapper;

        public ReviewController(IReviewServices reviewServices, IMapper mapper)
        {
            _reviewServices = reviewServices;
            _mapper = mapper;
        }

        [HttpPost]
        public async Task<ActionResult<ReviewViewModel>> Add([FromBody] CreateReviewModel createReview)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var review = _mapper.Map<Review>(createReview);

            if (review == null)
                return BadRequest("Invalid review data.");

            review = await _reviewServices.AddAsync(review);

            return Ok(_mapper.Map<ReviewViewModel>(review));
        }

        [HttpGet("product/{productItemId}")]
        public async Task<ActionResult<IEnumerable<ReviewViewModel>>> GetByProductItem(int productItemId)
        {
            var reviews = await _reviewServices.GetByProductItemIdAsync(productItemId);
            return Ok(_mapper.Map<List<ReviewViewModel>>(reviews));
        }

        [HttpGet("user/{userId}")]
        public async Task<ActionResult<IEnumerable<ReviewViewModel>>> GetByUser(string userId)
        {
            var reviews = await _reviewServices.GetByUserIdAsync(userId);
            return Ok(_mapper.Map<List<ReviewViewModel>>(reviews));
        }
    }
}