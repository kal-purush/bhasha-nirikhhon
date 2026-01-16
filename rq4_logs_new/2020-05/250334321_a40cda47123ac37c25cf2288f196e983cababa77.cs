using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SchedulearnBackend.DataAccessLayer;
using SchedulearnBackend.Models;
using SchedulearnBackend.Controllers.DTOs;
using SchedulearnBackend.Services;

namespace SchedulearnBackend.Controllers
{
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class LimitController : ControllerBase
    {
        private readonly LimitService _limitService;
        private readonly SchedulearnContext _schedulearnContext;

        public LimitController(LimitService limitService, SchedulearnContext schedulearnContext)
        {
            _limitService = limitService;
            _schedulearnContext = schedulearnContext;
        }

        // GET: api/Limit/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Limit>> GetLimit(int id)
        {
            System.Diagnostics.Debug.WriteLine($"GetLimit {id}");
            return await _limitService.GetLimitAsync(id);
        }
        // GET: api/Limit/user/5
        [HttpGet("user/{userId}")]
        public async Task<ActionResult<Limit>> GetLimitByUser(int userId)
        {
            System.Diagnostics.Debug.WriteLine($"GetLimitByUser {userId}");
            return await _limitService.GetLimitByUserAsync(userId);
        }


    }
}