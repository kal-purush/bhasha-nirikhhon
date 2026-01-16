using Microsoft.AspNetCore.Mvc;
using NIBM_Job_Portal.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NIBM_Job_Portal.Controllers
{
    public class JobController : Controller
    {

        private readonly ApplicationDbContext _applicationDbContext;
        public JobController(ApplicationDbContext applicationDbContext)
        {
  
            _applicationDbContext = applicationDbContext;
        }


        public ActionResult Index()
        {
            var jobs = _applicationDbContext.Job.ToList();
            return View(jobs);
        }

        [HttpGet]
        public ActionResult Create()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Create(Job model)
        {
            _applicationDbContext.Job.Add(model);
            _applicationDbContext.SaveChanges();
            ViewBag.Message = "Data Insert Successfully";
            return View();
        }
        [HttpGet]
        public ActionResult Edit(int id)
        {
            var data = _applicationDbContext.Job.Where(x => x.Id == id).FirstOrDefault();
            return View(data);
        }
        [HttpPost]
        public ActionResult Edit(Job model)
        {
            var data = _applicationDbContext.Job.Where(x => x.Id == model.Id).FirstOrDefault();
            if (data != null)
            {
                data.Position = model.Position;
                data.JobCategoryId = model.JobCategoryId;
                data.Description = model.Description;
                data.CompanyId = model.CompanyId;
                _applicationDbContext.SaveChanges();
            }

            return RedirectToAction("index");
        }
        public ActionResult Delete(int id)
        {
            var data = _applicationDbContext.Job.Where(x => x.Id == id).FirstOrDefault();
            _applicationDbContext.Job.Remove(data);
            _applicationDbContext.SaveChanges();
            ViewBag.Messsage = "Record Delete Successfully";
            return RedirectToAction("index");
        }
        public ActionResult View(int id)
        {
            var data = _applicationDbContext.Job.Where(x => x.Id == id).FirstOrDefault();
            return View(data);
        }

    }
}