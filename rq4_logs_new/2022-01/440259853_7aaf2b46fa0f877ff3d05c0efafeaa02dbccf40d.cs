using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MyAPI.DTOs;
using MyAPI.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MyAPI.Controllers
{
    [Route("api/danh-gia")]
    [ApiController]
    public class RatingController : ControllerBase
    {
        private readonly MyDbContext _db;
        private readonly UserManager<IdentityUser> _userManager;

        public RatingController(MyDbContext db, UserManager<IdentityUser> userManager)
        {
            _db = db;
            _userManager = userManager;
        }

        [HttpPost]
        public async Task<ActionResult> Post(RatingDTO ratingDTO)
        {
            var email = HttpContext.User.Claims.FirstOrDefault(a => a.Type == "email").Value;
            var thisUser = await _userManager.FindByEmailAsync(email);
            var userId = thisUser.Id;

            var currentRate = await _db.Ratings
                .FirstOrDefaultAsync(a => a.MovieId == ratingDTO.MovieId && a.UserId == userId);

            if (currentRate != null)
            {
                currentRate.RateStar = ratingDTO.RateStar;
            }
            else
            {
                var newRate = new Rating()
                {
                    MovieId = ratingDTO.MovieId,
                    RateStar = ratingDTO.RateStar,
                    UserId = userId
                };
                _db.Add(newRate);
            }

            await _db.SaveChangesAsync();
            return Ok();
        }
    }
}