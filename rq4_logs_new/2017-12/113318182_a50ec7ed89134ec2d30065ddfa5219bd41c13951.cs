using Microsoft.AspNet.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using TaskAssessor.Models;
using TaskAssessor.ViewModels;
using System.Data.Entity;



namespace TaskAssessor.Controllers
{
    public class MyJobsController : Controller
    {
        private ApplicationDbContext _context = new ApplicationDbContext();

        // GET: MyJobs
        public ActionResult Index()
        {
            var currentUserJobs = _context.HourIntervals.Include(u => u.Job).ToList().Where(u => u.ApplicationUserId == User.Identity.GetUserId());

            return View(currentUserJobs);
        }

        [Authorize]
        public ActionResult New()
        {
            var viewModel = new HourIntervalJobs()
            {
                HourInterval = new HourInterval(),
                Jobs = _context.Jobs.ToList()
            };
            return View("CreateMyJob", viewModel);
        }
        [HttpPost]
        public ActionResult CreateMyJob(HourInterval hourInterval)
        {
            if (hourInterval.Id == 0)
            {
                hourInterval.ApplicationUserId = User.Identity.GetUserId();
                hourInterval.DateAdded = DateTime.Now;
                _context.HourIntervals.Add(hourInterval);
            }
            _context.SaveChanges();
            return RedirectToAction("Index", "MyJobs");

        }


    }//class
}//namespace