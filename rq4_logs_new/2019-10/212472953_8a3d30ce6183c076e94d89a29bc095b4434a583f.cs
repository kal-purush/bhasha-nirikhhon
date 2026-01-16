using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using QuiryForum.Data;
using QuiryForum.Models;

namespace QuiryForum.Controllers
{
    public class PostsController : Controller
    {
        private readonly QuiryContext context;

        public PostsController(QuiryContext context)
        {
            this.context = context;
        }

        public IActionResult Index() 
        {
            return View();
        }

        public IActionResult Ask()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Ask(Post post)
        {
            if (ModelState.IsValid)
            {
                await QuestionDB.AddAsync(post, context);
                
                return RedirectToAction("Index");
            };

            // So that all of the errors that you encounter are sent with the view
            return View(post);
        }
    }
}