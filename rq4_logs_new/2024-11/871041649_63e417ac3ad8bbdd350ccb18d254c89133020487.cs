using backend.DTOs;
using backend.Repositories.Interfaces;
using backend.Services.Implementations;
using backend.Services.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace backend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CouponManagementController : ControllerBase
    {
        private readonly ICouponManagementService _couponManagementService;
        public CouponManagementController(ICouponManagementService _couponService)
        {
            _couponManagementService = _couponService;
        }
        [HttpPost("GenerateCouponCode")]
        public async Task<IActionResult> GenerateCouponCode([FromBody] CouponDto couponDto)
        {
            if (couponDto == null || couponDto.Expiry <= DateTime.UtcNow || couponDto.DiscountPercentage <= 0)
            {
                return BadRequest(new { errorMessage = "Please Sent A valid Data." });
            }
            try
            {
                string generatedCouponCode = await _couponManagementService.GenerateNewCouponCode(couponDto);
                if (string.IsNullOrEmpty(generatedCouponCode))
                {
                    return BadRequest(new { errorMessage = "Unable to generate Coupon Code. Please try again later" });
                }
                    return Ok(generatedCouponCode);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { errorMessage = "Internal Server Error.", ex.Message });
            }
        }

        [HttpGet("GetListOfCouponCodeByRestaurantId/{id:int}")]
        public async Task<IActionResult> GetCouponcodeListByRestaurant(int id)
        {
            if (id <= 0)
            {
                return BadRequest(new { errorMEssage = "Please enter valid Restaurant Id" });
            }
            try
            {
                GeneratedCouponsDto result = await _couponManagementService.GetCouponsCodeListByRestaurantId(id);
                if (result == null)
                {
                    return NotFound(new { errorMessage = "Unable to get Couponde Codes list." });
                }
                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { errorMessage = "Internal Server Error.", ex.Message });
            }

        }
        [HttpPatch("ExtendExpiryofCouponCode/{id:int}")]

        public async Task<IActionResult> UpdateCouponExpiryDate(int id, DateTime expiryDate)
        {
            if (id <= 0)
            {
                return BadRequest(new { errorMEssage = "Please Provide Valid CouponId" });
            }
            try
            {
                bool isExpiryUpdated = await _couponManagementService.UpdateCouponExpiry(id, expiryDate);
                if (isExpiryUpdated)
                {
                    return Ok(new { message = "Expiry Date Upadted Successfully" });
                }
                return BadRequest(new { errorMEssage = "Failed To Update Expiraty Date Coupon Code" });
            }
            catch (ArgumentException ex)
            {
                return BadRequest(new { errorMessage = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { errorMessage = "Internal Server Error.", ex.Message });
            }
           
        }

    }
}