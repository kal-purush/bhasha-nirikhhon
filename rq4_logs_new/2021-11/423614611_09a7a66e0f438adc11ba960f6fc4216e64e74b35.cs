using church_mgt_core.services.implementations;
using church_mgt_core.services.interfaces;
using church_mgt_dtos.PrayerRequestDtos;
using church_mgt_models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace church_mgt_api.Controllers
{
    [ApiController]
    [Route("api/[Controller]")]
    public class PrayerRequestsController : ControllerBase
    {
        private readonly IPrayerRequestService _prayerRequestService;
        private readonly UserManager<AppUser> _userManager;

        public PrayerRequestsController(IPrayerRequestService prayerRequestService, UserManager<AppUser> userManager)
        {
            _prayerRequestService = prayerRequestService;
            _userManager = userManager;
        }

        [HttpGet]
        public IActionResult PrayerRequests()
        {
            var result = _prayerRequestService.GetAllPrayerRequests();
            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("{prayerRequestId}")]
        public async Task<IActionResult> PrayerRequestById(string prayerRequestId)
        {
            var result = await _prayerRequestService.GetPrayerRequestByIdAsync(prayerRequestId);
            return StatusCode(result.StatusCode, result);
        }

        [HttpPost]
        [Authorize]
        public async Task<IActionResult> AddPrayerRequest(AddPrayerRequestDto prayerRequestDto)
        {
            var user = await _userManager.GetUserAsync(User);
            var result = await _prayerRequestService.AddPrayerRequest(user.Id, prayerRequestDto);
            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("memberId")]
        public IActionResult PrayerRequestsByMemberId(string memberId)
        {
            var result = _prayerRequestService.GetPrayerRequestsByMemberId(memberId);
            return StatusCode(result.StatusCode, result);
        }

        [HttpDelete("{prayerRequestId}")]
        public async Task<IActionResult> DeletePrayerRequestsById(string prayerRequestId)
        {
            var result = await _prayerRequestService.DeletePrayerRequestsById(prayerRequestId);
            return StatusCode(result.StatusCode, result);
        }
    }
}