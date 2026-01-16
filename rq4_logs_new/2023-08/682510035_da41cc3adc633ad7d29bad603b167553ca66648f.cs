using FPTManager.Entities;
using FPTManager.Models;
using FPTManager.Payloads;
using FPTManager.Payloads.Request;
using FPTManager.Services;
using FPTManager.Utils;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FPTManager.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LoginController : ControllerBase
    {
        private readonly AppSettings _appSettings;
        private readonly IAccountService _accountService;
        private readonly IStudentService _studentService;

        public LoginController(IOptionsMonitor<AppSettings> monitor, IAccountService accountService, IStudentService studentService)
        {
            _appSettings = monitor.CurrentValue;
            _accountService = accountService;
            _studentService = studentService;
        }

        [HttpPost]
        public IActionResult Login(LoginRequest loginRequest)
        {
            var isSucess =
                _accountService.Login(loginRequest.Username, loginRequest.Password);

            if(!isSucess)
            {
                return Unauthorized();
            }
            var account = _accountService.GetByUserName(loginRequest.Username);


            JwtHelper jwtHepler = new JwtHelper(_appSettings);
            var token = jwtHepler.GenerateToken(account);
            return Ok(token);
        }

        [HttpPut("SignUp")]
        public IActionResult SignUp(SignUpRequest signUpRequest)
        {

            var student = new Student
            {
                FirstName = signUpRequest.FirstName,
                MidName = signUpRequest.MidName,
                LastName = signUpRequest.LastName
            };

            var createStudentSucess = _studentService.AddStudent(student);

            if (createStudentSucess)
            {
                var createAccSucess = _accountService.SignUp(new Account
                {
                    Username = signUpRequest.UserName,
                    Password = signUpRequest.Password,
                    Email = signUpRequest.Email,
                    Student = student
                });

                if (createAccSucess)
                {
                    return Ok(new BaseResponse
                    {
                        StatusCode = StatusCodes.Status200OK,
                        Message = "Create Account Sucess"
                    });
                }
            }

            return BadRequest();
        }
    }
}