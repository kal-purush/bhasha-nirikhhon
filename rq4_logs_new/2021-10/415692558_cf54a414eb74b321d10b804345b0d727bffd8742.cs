using Auction.LMS.CORE.Data;
using Auction.LMS.CORE.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Auction.LMS.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class JwtController : Controller
    {
        private readonly IJwtService jwtService;
        public JwtController(IJwtService _jwtService)
        {
            jwtService = _jwtService;
        }
        [HttpPost]
        public IActionResult Authen([FromBody] Login login)
        {
            var token = jwtService.Auth(login);
            if (token == null)
            {
                return Unauthorized();
            }
            else
            {
                return Ok(token);
            }
        }
    }
}