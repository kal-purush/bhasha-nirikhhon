using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Online_Survey.Areas.Identity.Data;
using Online_Survey.DTOs.Account;
using Online_Survey.Services;
using System;
using System.Net.Http;
using System.Security.Claims;
using System.Threading.Tasks;

namespace Online_Survey.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        private readonly JWTServices _jwtService;
        private readonly SignInManager<Online_SurveyUser> _signInManager;
        private readonly UserManager<Online_SurveyUser> _userManager;
       
        public AccountController(JWTServices jwtServices, SignInManager<Online_SurveyUser> signInManager, UserManager<Online_SurveyUser> userManager)
        {
            _jwtService = jwtServices;
            _signInManager = signInManager;
            _userManager = userManager;
            

        }

        [HttpPost("Login")]
        public async Task<ActionResult<UserDto>> Login(LoginDto model)
        {
            var user = await _userManager.FindByNameAsync(model.UserName);
            if (user == null)
            {
                return Unauthorized("Invalid UserName or Password.");
            }

            if (user.EmailConfirmed == false) return Unauthorized("Please Confirm Your Email.");

            var result = await _signInManager.CheckPasswordSignInAsync(user, model.Password, false);
            if (!result.Succeeded)
            {
                return Unauthorized("Invalid UserName or Password.");
            }

            return  CreateApplicationUserDto(user);

        }

        [HttpPost("Register")]

        public async Task<ActionResult> Register(RegisterDto model)
        {
            if (await CheckEmailExistsAsync(model.Email))
            {
                return BadRequest($"{model.Email} Already Exists.");
            }

            var userToAdd = new Online_SurveyUser
            {
                FirstName = model.FirstName.ToLower(),
                LastName = model.LastName.ToLower(),
                UserName = model.Email.ToLower(),
                Email = model.Email.ToLower(),
                EmailConfirmed = true,
                Provider="Normal"

            };

            var result = await _userManager.CreateAsync(userToAdd, model.Password);

            if (!result.Succeeded) { return BadRequest(result.Errors); }



            return Ok(new JsonResult(new { title = "Account Created", message = "Your Account Has Been Created, Confirm your Email." }));

            /** try
               {
                   if (await SendConfirmEmailAsync(userToAdd))
                   {
                       return Ok(new JsonResult(new { title = "Account Created", message = "Your Account Has Been Created, Confirm your Email." }));
                   }
                   return BadRequest("Failed to send mail.Try to contact Admin.");
               }
               catch (Exception)
               {
                   return BadRequest("Failed to send mail.Try to contact Admin.");
               }
            **/



        }

        [Authorize]
        [HttpGet("refresh-user-token")]

        public async Task<ActionResult<UserDto>> RefreshUserToken()
        {
            var user = await _userManager.FindByNameAsync(User.FindFirst(ClaimTypes.Email)?.Value);
            return  CreateApplicationUserDto(user);
        }

        #region

        private UserDto CreateApplicationUserDto(Online_SurveyUser user)
        {
            return new UserDto
            {
                FirstName = user.FirstName,
                LastName = user.LastName,
                JWT =  _jwtService.CreateJWT(user),
            };
        }


        private async Task<bool> CheckEmailExistsAsync(string email)
        {
            return await _userManager.Users.AnyAsync(x => x.Email == email.ToLower());

        }

        #endregion
    }
}