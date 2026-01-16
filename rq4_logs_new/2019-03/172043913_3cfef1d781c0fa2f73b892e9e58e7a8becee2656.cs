using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Integratieproject1.BL.Managers;
using Integratieproject1.Domain;
using Integratieproject1.Domain.Ideations;
using Integratieproject1.Domain.Projects;
using Integratieproject1.Domain.Users;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Integratieproject1.UI.Controllers
{
    public class AdminController : Controller
    {
        private ProjectsManager projectsManager;
        private IdeationsManager ideationsManager;
        private SurveysManager surveysManager;
        private UsersManager usersManager;
        
        public AdminController()
        {
            projectsManager = new ProjectsManager();
            usersManager = new UsersManager();
            ideationsManager = new IdeationsManager();
            surveysManager = new SurveysManager();
        }
        public IActionResult Admin(int userId)
        {
            LoggedInUser user = usersManager.GetLoggedInUser(userId);
            return View("/UI/Views/Admin/Admin.cshtml", user );
        }
        public IActionResult Projects(int userId)
        {
            IList<Project> projects = projectsManager.GetProjects(userId);            
            return View("/UI/Views/Admin/Projects.cshtml" , projects);
        }

        public IActionResult Phases(int projectId)
        {
            IList<Phase> phases = projectsManager.GetPhases(projectId);
            return View("/UI/Views/Admin/Phases.cshtml" , phases);
        }
        public IActionResult EditProject(int projectId)
        {
            Project project = projectsManager.GetProject(projectId);
            return View("/UI/Views/Admin/EditProject.cshtml", project);
        }

        [HttpPost]
        public IActionResult EditProject(int projectId, int locationId,Project project)
        {
            projectsManager.EditProject(project, projectId, locationId);
            return RedirectToAction("Index","Home");
        }

        public IActionResult CreateProject(int userId)
        {
            LoggedInUser user = usersManager.GetLoggedInUser(userId);
            ViewData["PlatformId"] = user.Platform.PlatformId;
            return View("/UI/Views/Admin/CreateProject.cshtml");
        }
    }
}