using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Vedect.Data;

namespace Vedect.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SubscriptionPlansController : ControllerBase
    {
        private readonly AppDbContext _dbContext;

        public SubscriptionPlansController(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        [HttpGet("plans")]
        public async Task<IActionResult> GetPlans()
        {
            var plans = await _dbContext.SubscriptionPlans.ToListAsync();
            return Ok(plans);
        }
    }
}