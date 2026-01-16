using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using MvcCore.Models;
using BusinessLogic.Models;
using BusinessLogic.Containers;
using DAL.Contexts;
using MvcCore.Converters;
using BusinessLogic.Interfaces;

namespace MvcCore.Controllers
{
    public class CommentController : Controller
    {
        // 
        private readonly ILogicCommentContainer _commentContainer;
        private CommentViewConverter _CommentViewConverter = new CommentViewConverter();

        public CommentController(ILogicCommentContainer commentContainer)
        {
            _commentContainer = commentContainer;
        }



        [HttpPost]
        public IActionResult Create(CommentViewModel comment)
        {
            var modelCreate = _CommentViewConverter.Convert_To_CommentModel(comment);
            _commentContainer.CreateComment(modelCreate);
            return Redirect(Request.Headers["Referer"].ToString());
        }


        [HttpGet]
        public IActionResult Delete(int ID)
        {
            _commentContainer.DeleteComment(ID);
            return Redirect(Request.Headers["Referer"].ToString());
        }
    }
}