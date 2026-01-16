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
    public class HourIntervalsController : Controller
    {
        private readonly ApplicationDbContext _context = new ApplicationDbContext();

        private DateTime _today;
        public HourIntervalsController()
        {
            _today = DateTime.Now;
        }

        // GET: HourIntervals
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult New()
        {
            var viewModel = new HourIntervalJobs
            {
                HourInterval = new HourInterval(),
                Jobs = _context.Jobs.ToList()
            };
            return View("HourIntervalJobsForm",viewModel);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(HourInterval hourInterval)
        {
            if (hourInterval.Id == 0)
            _context.HourIntervals.Add(hourInterval);
            _context.SaveChanges();

            return RedirectToAction("Index","HourIntervals");
        }

        public ActionResult Edit(int id)
        {
            return View();
        }

        public ActionResult Delete(int id)
        {
            return View();
        }
    }
}