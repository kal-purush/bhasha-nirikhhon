using CaptoneProject_IOTS_BOs.DTO.GHTKDTO;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.GHTKController
{
    [Route("api/location")]
    [ApiController]
    public class LocationController : ControllerBase
    {
        private readonly IGHTKService _ghtkService;

        public LocationController(IGHTKService ghtkService)
        {
            _ghtkService = ghtkService;
        }

        [HttpGet("provinces")]
        public async Task<IActionResult> GetProvinces()
        {
            var provinces = await _ghtkService.SyncProvincesAsync();
            return Ok(provinces);
        }

        [HttpGet("districts")]
        public async Task<IActionResult> GetDistricts(int provinceId)
        {
            var provinces = await _ghtkService.SyncDistrictsAsync(provinceId);
            return Ok(provinces);
        }

        [HttpGet("wards")]
        public async Task<IActionResult> GetWards(int prodistrictId)
        {
            var provinces = await _ghtkService.SyncWardsAsync(prodistrictId);
            return Ok(provinces);
        }

        [HttpPost("calculate-fee")]
        public async Task<IActionResult> CalculateShippingFee()
        {
            var result = await _ghtkService.CalculateShippingFeeAsync();
            return Ok(result);
        }
    }
}