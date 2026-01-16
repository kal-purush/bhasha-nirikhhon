// AT 10-17-20
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using CISS411_Project.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace CISS411_Project.Controllers
{
    [Authorize(Roles ="Coach")]
    public class CoachController : Controller
    {
        private readonly SwimDbContext db;
        public CoachController(SwimDbContext db)
        {
            this.db = db;
        }
        public IActionResult Index()
        {
            return View();
        }
        // Add Profile
        public IActionResult AddProfile()
        {
            var currentUserId = this.User.FindFirst(ClaimTypes.NameIdentifier).Value;
            Coach coach = new Coach();
            if(db.Coaches.Any(c => c.UserId == currentUserId))
            {
                coach = db.Coaches.FirstOrDefault(c => c.UserId == currentUserId);
            }
            else
            {
                coach.UserId = currentUserId;
            }
            return View(coach);
        }
        [HttpPost]
        public async Task<IActionResult> AddProfile(Coach coach)
        {
            var currentUserId = this.User.FindFirst(ClaimTypes.NameIdentifier).Value;
            if (db.Coaches.Any(c => c.UserId == currentUserId))
            {
                var coachToUpdate = db.Coaches.FirstOrDefault(c => c.UserId == currentUserId);
                coachToUpdate.Name = coach.Name;
                coachToUpdate.PhoneNumber = coach.PhoneNumber;
                db.Update(coachToUpdate);
            }
            else
            {
                db.Add(coach);
            }
            await db.SaveChangesAsync();
            return View("Index");
        }
        //
    }
}