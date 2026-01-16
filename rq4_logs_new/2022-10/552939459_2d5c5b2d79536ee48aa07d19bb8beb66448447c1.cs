using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using Vacancy.Repository.Dto.UserDto;
using Vacancy.Repository.Repositories;
using Vakansion.Core;
using Vakansion.Models;


namespace Vakansion.Controllers
{
    
        [Authorize(Roles = "Admin")]
        public class UsersController : Controller
        {
            private readonly UsersRepository usersRepository;
            public UsersController(UsersRepository usersRepository)
            {
                this.usersRepository = usersRepository;
            }

            public async Task<IActionResult> Index()
            {
                return View(await usersRepository.GetUsersAsync());
            }

            [HttpGet]
            public IActionResult Create()
            {
                return View();
            }

            [HttpPost]
            [AutoValidateAntiforgeryToken]
            public async Task<IActionResult> Create(UserCreate model)
            {
                if (ModelState.IsValid)
                {
                    User user = await usersRepository.CreateUserAsync(model.FirstName, model.LastName, model.Password, model.Email);
                    return RedirectToAction("Edit", "Users", new { id = user.Id });
                }
                return View(model);
            }

            [HttpGet]
            public async Task<IActionResult> Delete(string id)
            {
                return View(await usersRepository.GetUserAsync(id));
            }

            [HttpPost]
            [AutoValidateAntiforgeryToken]
            public async Task<IActionResult> ConfirmDelete(string id)
            {
                await usersRepository.DeleteUserAsync(id);
                return RedirectToAction("Index");
            }

            [HttpGet]
            public async Task<IActionResult> Edit(string id)
            {
                ViewBag.Roles = await usersRepository.GetRolesAsync();
                return View(await usersRepository.GetUserAsync(id));
            }

            [HttpPost]
            [AutoValidateAntiforgeryToken]
            public async Task<IActionResult> Edit(UserRead model, string[] roles)
            {
                if (ModelState.IsValid)
                {
                    await usersRepository.UpdateAsync(model, roles);
                    return RedirectToAction("Index");
                }
                ViewBag.Roles = await usersRepository.GetRolesAsync();
                return View(model);
            }

            [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
            public IActionResult Error()
            {
                return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
            }
        }
}
