using Forum.Models.DTOs;
using Forum.Services;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;

namespace Forum.Controllers
{
    public class UserController : Controller
    {
        private IUserService userService;

        public UserController(IUserService userService)
        {
            this.userService = userService;
        }

        public IActionResult Index()
        {
            List<UserDTO> users = userService.GetAll();

            return View(users);
        }

        [HttpGet]
        public IActionResult Create()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Create(UserDTO user)
        {
            userService.Create(user);

            return RedirectToAction(nameof(Index));
        }

        [HttpGet]
        public IActionResult Edit(int id)
        {
           UserDTO user =  userService.GetById(id);

            return View(user);
        }

        [HttpPost]
        public IActionResult Edit(int id, UserDTO user)
        {
            userService.Edit(id, user);

            return RedirectToAction(nameof(Index));
        }

        [HttpGet]
        public IActionResult Delete(int id)
        {
            UserDTO user = userService.GetById(id);

            return View(user);
        }

        [HttpPost]
        public IActionResult DeleteConfirm(int id)
        {
            userService.Delete(id);

            return RedirectToAction(nameof(Index));
        }



    }
}