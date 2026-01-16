using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using SocialClubApp.Interfaces;
using SocialClubApp.Models;
using SocialClubApp.Services;
using SocialClubApp.ViewModels;

namespace SocialClubApp.Controllers
{
    [Authorize]
    public class UserController : Controller
    {
        private readonly IUserRepository _userRepository;
        private readonly UserManager<AppUser> _userManager;
        private readonly SignInManager<AppUser> _signInManager;
        private readonly IPhotoService _photoService;

        public UserController(IUserRepository userRepository, UserManager<AppUser> userManager, SignInManager<AppUser> signInManager, IPhotoService photoService)
        {
            _userRepository = userRepository;
            _userManager = userManager;
            _signInManager = signInManager;
            _photoService = photoService;
        }


        public IActionResult Index()
        {
            return View();
        }

        [HttpGet]
        public async Task<IActionResult> Details(string id)
        {
            var user = await _userRepository.GetUserByIdAsync(id);
            var userClaims = await _userManager.GetClaimsAsync(user);

            if (user == null)
            {
                return View("Error");
            }

            var detailsVM = new UserDetailsViewModel()
            {
                UserName = user.UserName,
                Email = user.Email,
                City = user.City,
                State = user.State,
                ProfileImageUrl = user.ProfileImageUrl,
                Claims = userClaims.Select(c => c.Value).ToList()
            };

            return View(detailsVM);
        }

        [HttpGet]
        public async Task<IActionResult> Edit() 
        {
            var user = await _userManager.GetUserAsync(User);

            if (user == null) 
            {
                return View("Error");
            }

            var userVM = new EditUserViewModel()
            {                
                Id = user.Id,
                UserName = user.UserName,
                Email = user.Email,
                City = user.City,
                State = user.State,
                ProfileImageUrl = user.ProfileImageUrl
            };

            return View(userVM);
        }

        [HttpPost]
        public async Task<IActionResult> Edit(EditUserViewModel editUserVM) 
        {
            var user = await _userManager.GetUserAsync(User);

            if (user == null)
            {
                return View("Error");
            }

            if (!ModelState.IsValid)
            {
                ModelState.AddModelError("", "Failed to edit profile");

                // Need to set the ProfileImageUrl because the editVM.ProfileImageUrl is null when user come from the profile information form and not the upload image form
                editUserVM.ProfileImageUrl = user.ProfileImageUrl;

                return View("Edit", editUserVM);
            }

            if (editUserVM.Image != null) // only update profile image
            {
                var photoResult = await _photoService.AddUserPhotoAsync(editUserVM.Image);

                if (photoResult.Error != null)
                {
                    ModelState.AddModelError("Image", "Failed to upload image");
                    return View("Edit", editUserVM);
                }

                if (!string.IsNullOrEmpty(user.ProfileImageUrl))
                {
                    _ = _photoService.DeletePhotoAsync(user.ProfileImageUrl);
                }

                user.ProfileImageUrl = photoResult.Url.ToString();
                editUserVM.ProfileImageUrl = user.ProfileImageUrl;

                await _userManager.UpdateAsync(user);
                                
                return RedirectToAction("Details", "User", new { user.Id });
            }

            user.UserName = editUserVM.UserName;
            user.City = editUserVM.City;
            user.State = editUserVM.State;                     

           await _userManager.UpdateAsync(user);

           return RedirectToAction("Details", "User", new { user.Id });
        }

        [HttpGet]
        public async Task<IActionResult> Delete(string id)
        {           
            var user = await _userRepository.GetUserByIdAsync(id);

            if (user == null)
            {
                return View("Error");
            }

           
            return View(user);
        }

        [HttpPost, ActionName("Delete")]
        [HttpPost]
        public async Task<IActionResult> DeleteUser(string id)
        {
            var user = await _userRepository.GetUserByIdAsync(id);

            if (user == null)
            {
                return View("Error");
            }

            await _signInManager.SignOutAsync();
            _userRepository.Delete(user);

            return RedirectToAction("Index", "Home");
        }

    }
}