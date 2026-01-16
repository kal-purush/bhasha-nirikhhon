using GryGiereczki.Data;
using GryGiereczki.Services;
using GryGiereczki.ViewModels;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.IO;

namespace GryGiereczki.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserEditController : ControllerBase
    {
        private readonly IUserRepository _repository;
        private readonly JwtService _jwtService;
        private readonly IWebHostEnvironment _env;
        public UserEditController(IUserRepository repository, JwtService jwtService, IWebHostEnvironment env)
        {
            _repository = repository;
            _jwtService = jwtService;
            _env = env;
        }

        [HttpPut("editUser")]
        public IActionResult EditUser(UserEditVM userEditVM)
        {



            try
            {
                var jwt = Request.Cookies["jwt"];

                var token = _jwtService.Verify(jwt);

                int userId = int.Parse(token.Issuer);

                var user = _repository.GetById(userId);



                var userEmail = _repository.GetByEmail(userEditVM.Email);
                if (userEmail != null) return BadRequest(new { message = "User with this email already exists" });

                var userNick = _repository.GetByNick(userEditVM.Nick);
                if (userNick != null) return BadRequest(new { message = "User with this nick already exists" });


                if (userEditVM.Nick == null) { user.Nick = user.Nick; }
                else { user.Nick = userEditVM.Nick; }

                if (userEditVM.Email == null) { user.Email = user.Email; }
                else { user.Email = userEditVM.Email; }

                if (userEditVM.Name == null) { user.Name = user.Name; }
                else { user.Name = userEditVM.Name; }

                if (userEditVM.Lastname == null) { user.Lastname = user.Lastname; }
                else { user.Lastname = userEditVM.Lastname; }

                if (userEditVM.DateOfBirth == null) { user.DateOfBirth = user.DateOfBirth; }
                else { user.DateOfBirth = userEditVM.DateOfBirth; }

                if (userEditVM.Description == null) { user.Description = user.Description; }
                else { user.Description = userEditVM.Description; }




                _repository.SaveUserEdit(user);



                return Ok(new
                {
                    message = "success edit"
                });

            }
            catch (Exception _)
            {
                return Unauthorized();
            }
        }

        [HttpPut("editPassword")]
        public IActionResult EditUserPassword(UserEditPassword userEditPassword)
        {

            try
            {
                var jwt = Request.Cookies["jwt"];

                var token = _jwtService.Verify(jwt);

                int userId = int.Parse(token.Issuer);

                var user = _repository.GetById(userId);

                if (BCrypt.Net.BCrypt.Verify(userEditPassword.Password, user.Password))
                {
                    user.Password = BCrypt.Net.BCrypt.HashPassword(userEditPassword.NewPassword);
                }
                else { return BadRequest(new { message = "Invalid password" }); }

                _repository.SaveUserEdit(user);



                return Ok(new
                {
                    message = "success edit"
                });

            }
            catch (Exception _)
            {
                return Unauthorized();
            }
        }

        [Route("avatar")]
        [HttpPost]
        public JsonResult SaveFile()
        {
            try
            {
                var httpRequest = Request.Form;
                var postedFile = httpRequest.Files[0];
                string filename=postedFile.FileName;
                var physicalPath = _env.ContentRootPath + "/Photos/" + filename;

                using (var stream=new FileStream(physicalPath, FileMode.Create))
                {
                    postedFile.CopyTo(stream);
                }
                return new JsonResult(filename);
            }
            catch (Exception)
            {

                return new JsonResult("test.jpg");
            }
        }

    }
}