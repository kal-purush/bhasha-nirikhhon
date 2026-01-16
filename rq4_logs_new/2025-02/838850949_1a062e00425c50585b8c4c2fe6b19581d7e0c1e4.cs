using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using ModelLibrary.Dto;
using Newtonsoft.Json;
using Web.Service.IService;
using Web.Utility;

namespace Web.Controllers
{
    public class AuthController : Controller
    {
        private readonly IAuthService _authService;

        public AuthController(IAuthService authService)
        {
            _authService = authService;
        }

        [HttpGet]
        public IActionResult Login()
        {
            LoginRequestDto loginRequestDto = new();
            return View(loginRequestDto);
        }

        [HttpPost]
        public async Task<IActionResult> Login(LoginRequestDto loginRequestDto)
        {
            var responseDto = await _authService.LoginAsync(loginRequestDto);
            if (responseDto != null && responseDto.IsSuccess)
            {
                LoginResponseDto loginResponseDto = 
                    JsonConvert.DeserializeObject<LoginResponseDto>(Convert.ToString(responseDto.Result));
                TempData["success"] = "Login Successful!";
                return RedirectToAction(nameof(HomeController.Index), nameof(HomeController));
            }
            //TempData["error"] = "Login Error: " + responseDto.Message;
            ModelState.AddModelError("CustomError", responseDto.Message);
            return View(loginRequestDto);
        }

        [HttpGet]
        public IActionResult Register()
        {
            var roleList = new List<SelectListItem>()
            {
                new SelectListItem{Text  = StaticDetails.RoleAdmin, Value = StaticDetails.RoleAdmin},
                new SelectListItem{Text  = StaticDetails.RoleCustomer, Value = StaticDetails.RoleCustomer},
            };
            ViewBag.RoleList = roleList;
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Register(RegistrationRequestDto registrationRequestDto)
        {
            var responseDto = await _authService.RegisterAsync(registrationRequestDto);
            if (responseDto != null && responseDto.IsSuccess)
            {
                if (string.IsNullOrEmpty(registrationRequestDto.RoleName))
                {
                    registrationRequestDto.RoleName = StaticDetails.RoleCustomer;
                }

                var responseRoleDto = await _authService.AssignRoleAsync(registrationRequestDto);
                if (responseRoleDto != null && responseRoleDto.IsSuccess)
                {
                    TempData["success"] = "Registration Successful!";
                    return RedirectToAction(nameof(Login));
                }
            }
            var roleList = new List<SelectListItem>()
            {
                new SelectListItem{Text  = StaticDetails.RoleAdmin, Value = StaticDetails.RoleAdmin},
                new SelectListItem{Text  = StaticDetails.RoleCustomer, Value = StaticDetails.RoleCustomer},
            };
            ViewBag.RoleList = roleList;
            TempData["error"] = "Registration Error: " + responseDto.Message;
            return View(registrationRequestDto);
        }

        public IActionResult Logout()
        {
            return View();
        }
    }
}