using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OnlineStore.Domain.Entities;

namespace OnlineStore.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SellersController : ControllerBase
    {
        private readonly OnlinestoreContext _onlinestoreContext;

        public SellersController(OnlinestoreContext onlinestoreContext)
        {
            _onlinestoreContext = onlinestoreContext;
        }

        [HttpGet]
        public async Task<ActionResult<List<Seller>>> GetSellers()
        {
            return Ok(await _onlinestoreContext.Sellers.ToListAsync());
        }
    }
}