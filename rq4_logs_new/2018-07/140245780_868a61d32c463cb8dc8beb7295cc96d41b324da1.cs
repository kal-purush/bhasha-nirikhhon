using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using System.Abstract;

namespace C2C2.Controllers
{
    public class AdminController : Controller
    {
        private IQuestionRepository repository;

        public ActionResult Index()
        {
            return View(repository.Questions);
        }

        public AdminController(IQuestionRepository repo)
        {
            repository = repo;
        }
    }
}