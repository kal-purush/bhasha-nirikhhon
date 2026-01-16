using backend.DTOs;
using backend.Models;
using backend.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace backend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReviewController : ControllerBase
    {
        private readonly IReviewService _reviewService;

        public ReviewController(IReviewService reviewService)
        {
            _reviewService = reviewService;
        }

        [HttpPost]
        public async Task<IActionResult> AddReview([FromBody] AddReviewDto review)
        {
            if (review == null)
            {
                return BadRequest("Review is null.");
            }

            var createdReview = await _reviewService.AddReviews(review);
            return CreatedAtAction(nameof(AddReview), new { id = createdReview.Id }, createdReview);
        }

        [HttpGet("restaurantRating/{restaurantId}")]
        public async Task<IActionResult> GetRestaurantRating(int restaurantId)
        {
            if(restaurantId == 0 || restaurantId==null) { return BadRequest(); }

            var restaurantRating = await _reviewService.GetRestaurantRating(restaurantId);

            if (restaurantRating == null)
            {
                return NotFound($"No ratings found for restuarant with ID {restaurantId}");
            }
            return Ok(new { restaurantId = restaurantId, restaurantRating= restaurantRating });
        }
    }
}