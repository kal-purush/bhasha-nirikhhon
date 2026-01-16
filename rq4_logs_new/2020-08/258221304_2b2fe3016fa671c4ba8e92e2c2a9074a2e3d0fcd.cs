using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace GameSource.Areas.User.Controllers
{
    [Area("User")]
    [Route("user/profile/comment")]
    public class UserProfileCommentController : Controller
    {
        public UserProfileCommentController()
        {

        }

        [HttpGet("create")]
        public IActionResult Create(ViewResult viewModel)
        {
            return View();
        }

        [HttpPost("create")]
        [ValidateAntiForgeryToken]
        public IActionResult Create()
        {
            return View();
        }

        [HttpGet("edit/{id}")]
        public IActionResult Edit(ViewResult viewModel)
        {
            return View();
        }

        [HttpPost("edit/{id}")]
        [ValidateAntiForgeryToken]
        public IActionResult Edit(int? id)
        {
            return View();
        }

        [HttpGet("delete/{id}")]
        public IActionResult Delete(ViewResult viewModel)
        {
            return View();
        }

        [HttpPost("delete/{id}")]
        [ValidateAntiForgeryToken]
        public IActionResult Delete(int? id)
        {
            return View();
        }
    }
}