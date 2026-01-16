using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using WebApp.ViewModels;

namespace WebApp.Controllers
{
    public class UserAssignmentController : AppController
    {
        [HttpGet]
        [Route("[controller]/{assignmentId}")]
        public async Task<IActionResult> AddUserAssignment(int assignmentId)
        {
            string message = await GetUser();
            var model = new UserAssignmentViewModel();
            var response = await APIGetAssignmentById(assignmentId);
            if (response.Data != null)
            {
                model.Assignment = response.Data;
            }
            return View(model);
        }
    }
}