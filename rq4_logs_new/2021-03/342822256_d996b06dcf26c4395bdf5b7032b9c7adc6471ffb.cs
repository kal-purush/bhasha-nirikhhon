using HostelWebAPI.DTOs;
using HostelWebAPI.Models;
using HostelWebAPI.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace HostelWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : ControllerBase
    {

        public UsersController(UserManager<User> userManager, SignInManager<User> signInManager, ITokenService tokenService)
        {
            this.userManager = userManager;
            this.signInManager = signInManager;
            this.tokenService = tokenService;
        }
        private readonly UserManager<User> userManager;
        private readonly SignInManager<User> signInManager;
        private readonly ITokenService tokenService;

        [HttpPost]
        [Route("registerUser")]
        public async Task<IActionResult> RegisterUserAsync()
        {
            User user = new User()
            {
                Name = "User 1",
                Email = "user1@mail.com",
                UserName = "user1@mail.com"
            };

            var res = await userManager.CreateAsync(user, "password");
            if (res.Succeeded)
            {
                await userManager.AddToRoleAsync(user, "User");
                await signInManager.SignInAsync(user, true);
                return Ok(new ReturnMsg($"User {user.Name} registered!", null));
            }
            foreach (var error in res.Errors)
            {
                ModelState.AddModelError(string.Empty, error.Description);
            }
            return Ok(new ReturnMsg("", ModelState.Values.SelectMany(e => e.Errors.Select(er => er.ErrorMessage))));
        }

        [HttpPost]
        [Authorize(Roles = "User", Policy = "Bearer")]
        [Route("registerOwner/{id}")]
        public async Task<IActionResult> RegisterOwnerAsync([FromRoute]string id)
        {
            var user = await userManager.FindByIdAsync(id);
            if(user != null)
            {
                userManager.AddToRoleAsync(user, "Owner").Wait();
            }
            return Ok();
        }

        [HttpGet]
        [Authorize]
        [Route("test-authen")]
        public async Task<object> Test()
        {
            var email = HttpContext.User.Claims.FirstOrDefault(c => c.Type == ClaimTypes.Email).Value;
            var user = await userManager.FindByEmailAsync(email);
            return user.UserName;
        }

        [HttpPost]
        [AllowAnonymous]
        [Route("authenticate")]
        public async Task<IActionResult> Authenticate([FromBody] LoginFormDTO payload)
        {
            if (ModelState.IsValid) {
                var result = await signInManager.PasswordSignInAsync(payload.Email, payload.Password, true, false);
                if (result.Succeeded)
                {
                    var user = await userManager.FindByEmailAsync(payload.Email);
                    var roles = await userManager.GetRolesAsync(user);
                    var token = tokenService.TokenGenerator(user, roles);
                    return Ok(new ReturnTokenDTO(user.Id, token));
                }
            }
            return BadRequest("Something's wrong");
        }
    }
}