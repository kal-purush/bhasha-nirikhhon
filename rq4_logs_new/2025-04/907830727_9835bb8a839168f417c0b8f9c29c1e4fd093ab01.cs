using AutoMapper;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Services;
using EasyTravel.Web.Areas.Profile.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Runtime.InteropServices;
using System.Security.Claims;

namespace EasyTravel.Web.Areas.Profile.Controllers
{
    [Area("Profile"),Authorize]
    public class MyAccountController : Controller
    {
        private readonly IAdminUserService _adminUserService;
        private readonly IMapper _mapper;
        public MyAccountController(IAdminUserService adminUserService,IMapper mapper)
        {
            _adminUserService = adminUserService;
            _mapper = mapper;
        }
        public IActionResult Index()
        {
            return View();
        }
        [HttpGet(Name = "personal-info")]
        public async Task<IActionResult> PersonalInfo()
        {
            var user = await _adminUserService.GetAsync(Guid.Parse(User.FindFirst(ClaimTypes.NameIdentifier)?.Value));
            var model = _mapper.Map<ProfileUserModel>(user);
            return View(model);
        }
        [HttpPost(Name = "personal-info")]
        public async Task<IActionResult> PersonalInfo(ProfileUserModel model)
        {
            if (ModelState.IsValid)
            {
                var user = await _adminUserService.GetAsync(model.Id);
                user = _mapper.Map(model, user);
                var (success, errorMessage) = await _adminUserService.UpdateAsync(user);
                if (!success)
                {
                    ModelState.AddModelError(string.Empty, errorMessage);
                    return View(model);
                }
                return RedirectToAction("PersonalInfo", "MyAccount", new { area = "Profile" });
            }
            return View(model);
        }
    }
}