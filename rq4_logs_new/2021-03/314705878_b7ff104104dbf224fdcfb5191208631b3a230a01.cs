using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Identity;
using CS295_Term.Models;
using CS295_Term.Repositories;

namespace CS295_Term.Controllers
{
    [Authorize(Roles = "Admin")]
    public class AdminController : Controller
    {

        private UserManager<SiteUser> userManager;
        private RoleManager<IdentityRole> roleManager;
        private IRecipeRepository recipeRepo;

        public AdminController(UserManager<SiteUser> userMngr,
                               RoleManager<IdentityRole> roleMngr,
                               IRecipeRepository repo)
        {
            userManager = userMngr;
            roleManager = roleMngr;
            recipeRepo = repo;
        }

        public async Task<IActionResult> Index()
        {
            List<SiteUser> users = new List<SiteUser>();
            foreach (SiteUser user in userManager.Users)
            {
                user.RoleNames = await userManager.GetRolesAsync(user);
                users.Add(user);
            }

            AdminViewModel model = new AdminViewModel
            {
                Users = users, // List of AppUsers
                Roles = roleManager.Roles
            };

            return View(model);
        }
        [HttpPost]
        public async Task<IActionResult> Delete(string id)
        {
            IdentityResult result = null;
            SiteUser user = await userManager.FindByIdAsync(id);
            if (user != null)
            {
                // Check to see if the user has posted a recipe
                if (0 == (from r in recipeRepo.Recipes
                          where r.User.UserName == user.UserName
                          select r).Count<Recipe>())
                {

                    result = await userManager.DeleteAsync(user);
                }
                else
                {
                    result = IdentityResult.Failed(new IdentityError()
                    { Description = "User's recipes must be deleted first." });
                }

                if (!result.Succeeded)
                {
                    // if failed 
                    string errorMessage = "";
                    foreach (IdentityError error in result.Errors)
                    {
                        errorMessage += errorMessage != "" ? " | " : "";   // put a separator between messages
                        errorMessage += error.Description;
                    }
                    TempData["message"] = errorMessage;
                }
                else
                {
                    TempData["message"] = "";  // No errors, clear the message
                }
            }
            return RedirectToAction("Index");
        }

        [HttpPost]
        public async Task<IActionResult> AddToAdmin(string id)
        {
            IdentityRole adminRole = await roleManager.FindByNameAsync("Admin");
            if (adminRole == null)
            {
                TempData["message"] = "Admin role does not exist. "
                    + "Click 'Create Admin Role' button to create it.";
            }
            else
            {
                SiteUser user = await userManager.FindByIdAsync(id);
                await userManager.AddToRoleAsync(user, adminRole.Name);
            }
            return RedirectToAction("Index");
        }

        [HttpPost]
        public async Task<IActionResult> RemoveFromAdmin(string id)
        {
            SiteUser user = await userManager.FindByIdAsync(id);
            var result = await userManager.RemoveFromRoleAsync(user, "Admin");
            if (result.Succeeded) { }
            return RedirectToAction("Index");
        }

        //For stuff with roles


        [HttpPost]
        public async Task<IActionResult> DeleteRole(string id)
        {
            IdentityRole role = await roleManager.FindByIdAsync(id);
            await roleManager.DeleteAsync(role);
            return RedirectToAction("Index");
        }

        [HttpPost]
        public async Task<IActionResult> CreateAdminRole()
        {
            await roleManager.CreateAsync(new IdentityRole("Admin"));
            return RedirectToAction("Index");
        }

    }
}