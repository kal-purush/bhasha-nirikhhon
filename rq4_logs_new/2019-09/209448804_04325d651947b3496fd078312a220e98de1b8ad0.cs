using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Claims;
using System.Threading.Tasks;
using Amazon.S3;
using AspNet.Security.OAuth.Validation;
using AttorneyJournal.Data;
using AttorneyJournal.Helpers;
using AttorneyJournal.Models;
using AttorneyJournal.Models.AccountViewModels;
using AttorneyJournal.Models.ResourceModels;
using AttorneyJournal.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Stripe;

namespace AttorneyJournal.Controllers
{
	[Authorize]
	public class AccountController : Controller
	{
		private readonly UserManager<ApplicationUser> _userManager;
		private readonly SignInManager<ApplicationUser> _signInManager;
		private readonly IEmailSender _emailSender;
		private readonly ISmsSender _smsSender;
		private readonly IAmazonS3 _amazonS3;
		private readonly ApplicationDbContext _applicationDbContext;
		private static bool _databaseChecked;

		public AccountController(
			UserManager<ApplicationUser> userManager,
			SignInManager<ApplicationUser> signInManager,
			IEmailSender emailSender,
			ISmsSender smsSender,
			ApplicationDbContext applicationDbContext,
			IOptions<IdentityOptions> identityOptions,
			IAmazonS3 amazonS3)
		{
			_userManager = userManager;
			_signInManager = signInManager;
			_emailSender = emailSender;
			_smsSender = smsSender;
			_applicationDbContext = applicationDbContext;
			_amazonS3 = amazonS3;
		}

		//
		// GET: /Account/Login
		[HttpGet]
		[AllowAnonymous]
		public IActionResult Login(string returnUrl = null)
		{
			ViewData["ReturnUrl"] = returnUrl;
			return View();
		}

		//
		// POST: /Account/Login
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> Login(LoginViewModel model, string returnUrl = null)
		{
			EnsureDatabaseCreated(_applicationDbContext);
			ViewData["ReturnUrl"] = returnUrl;
			if (ModelState.IsValid)
			{
				// This doesn't count login failures towards account lockout
				// To enable password failures to trigger account lockout, set lockoutOnFailure: true
				var result = await _signInManager.PasswordSignInAsync(model.Email, model.Password, model.RememberMe, false);
				if (result.Succeeded)
				{
					return RedirectToLocal(returnUrl);
				}
				if (result.RequiresTwoFactor)
				{
					return RedirectToAction(nameof(SendCode), new { ReturnUrl = returnUrl, model.RememberMe });
				}
				if (result.IsLockedOut)
				{
					return View("Lockout");
				}
				ModelState.AddModelError(string.Empty, "Invalid login attempt.");
				return View(model);
			}

			// If we got this far, something failed, redisplay form
			return View(model);
		}

        [HttpPost]
        [AllowAnonymous]
        public IActionResult PaymentStrip()
        {
            StripeConfiguration.ApiKey = "sk_test_slG5nYGhZM40uMPptHs7GTzL00ptIeUVTT";

            var paymentIntentService = new PaymentIntentService();
            var createOptions = new PaymentIntentCreateOptions
            {
                Amount = 9,
                Currency = "usd",
                PaymentMethodTypes = new List<string> { "card" },
                ReceiptEmail = "abc.rosen@example.com",
            };
            paymentIntentService.Create(createOptions);
            return Ok();
        }

        //
        // GET: /Account/Register
        [HttpGet]
		[AllowAnonymous]
		public IActionResult Register(string returnUrl = null)
		{
			return Unauthorized();
			// ViewData["ReturnUrl"] = returnUrl;
			// return View();
		}

		//
		// POST: /Account/Register
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> Register(RegisterViewModel model, string returnUrl = null)
		{
			return Unauthorized();

			EnsureDatabaseCreated(_applicationDbContext);
			ViewData["ReturnUrl"] = returnUrl;
			if (ModelState.IsValid)
			{
				var user = new ApplicationUser { UserName = model.Email, Email = model.Email };
				var result = await _userManager.CreateAsync(user, model.Password);
				if (result.Succeeded)
				{
					// For more information on how to enable account confirmation and password reset please visit http://go.microsoft.com/fwlink/?LinkID=532713
					// Send an email with this link
					var code = await _userManager.GenerateEmailConfirmationTokenAsync(user);
					var callbackUrl = Url.Action("ConfirmEmail", "Account", new { userId = user.Id, code }, Request.Scheme);

					await _emailSender.SendEmailAsync("", model.Email, "Confirm your account", "Please confirm your account by clicking this link: <a href=\"" + callbackUrl + "\">link</a>");
					await _signInManager.SignInAsync(user, false);
					return RedirectToLocal(returnUrl);
				}
				AddErrors(result);
			}

			// If we got this far, something failed, redisplay form
			return View(model);
		}

		//
		// POST: /Account/Logout
		[HttpPost]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> Logout()
		{
			await _signInManager.SignOutAsync();
			return RedirectToAction(nameof(HomeController.Index), "Home");
		}

		//
		// POST: /Account/ExternalLogin
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public IActionResult ExternalLogin(string provider, string returnUrl = null)
		{
			EnsureDatabaseCreated(_applicationDbContext);
			// Request a redirect to the external login provider.
			var redirectUrl = Url.Action("ExternalLoginCallback", "Account", new { ReturnUrl = returnUrl });
			var properties = _signInManager.ConfigureExternalAuthenticationProperties(provider, redirectUrl);
			return new ChallengeResult(provider, properties);
		}

		//
		// GET: /Account/ExternalLoginCallback
		[HttpGet]
		[AllowAnonymous]
		public async Task<IActionResult> ExternalLoginCallback(string returnUrl = null)
		{
			var info = await _signInManager.GetExternalLoginInfoAsync();
			if (info == null)
			{
				return RedirectToAction(nameof(Login));
			}

			// Sign in the user with this external login provider if the user already has a login.
			var result = await _signInManager.ExternalLoginSignInAsync(info.LoginProvider, info.ProviderKey, isPersistent: false);
			if (result.Succeeded)
			{
				return RedirectToLocal(returnUrl);
			}
			if (result.RequiresTwoFactor)
			{
				return RedirectToAction(nameof(SendCode), new { ReturnUrl = returnUrl });
			}
			if (result.IsLockedOut)
			{
				return View("Lockout");
			}
			// If the user does not have an account, then ask the user to create an account.
			ViewData["ReturnUrl"] = returnUrl;
			ViewData["LoginProvider"] = info.LoginProvider;
			var email = info.Principal.FindFirstValue(ClaimTypes.Email);
			return View("ExternalLoginConfirmation", new ExternalLoginConfirmationViewModel { Email = email });
		}

		//
		// POST: /Account/ExternalLoginConfirmation
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> ExternalLoginConfirmation(ExternalLoginConfirmationViewModel model, string returnUrl = null)
		{
			if (ModelState.IsValid)
			{
				// Get the information about the user from the external login provider
				var info = await _signInManager.GetExternalLoginInfoAsync();
				if (info == null)
				{
					return View("ExternalLoginFailure");
				}
				var user = new ApplicationUser { UserName = model.Email, Email = model.Email };
				var result = await _userManager.CreateAsync(user);
				if (result.Succeeded)
				{
					result = await _userManager.AddLoginAsync(user, info);
					if (result.Succeeded)
					{
						await _signInManager.SignInAsync(user, false);
						return RedirectToLocal(returnUrl);
					}
				}
				AddErrors(result);
			}

			ViewData["ReturnUrl"] = returnUrl;
			return View(model);
		}

		// GET: /Account/ConfirmEmail
		[HttpGet]
		[AllowAnonymous]
		public async Task<IActionResult> ConfirmEmail(string userId, string code)
		{
			if (userId == null || code == null)
			{
				return View("Error");
			}
			var user = await _userManager.FindByIdAsync(userId);
			if (user == null)
			{
				return View("Error");
			}
			var result = await _userManager.ConfirmEmailAsync(user, code);
			return View(result.Succeeded ? "ConfirmEmail" : "Error");
		}

		//
		// GET: /Account/ForgotPassword
		[HttpGet]
		[AllowAnonymous]
		public IActionResult ForgotPassword()
		{
			return View();
		}

		//
		// POST: /Account/ForgotPassword
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> ForgotPassword(ForgotPasswordViewModel model)
		{
			if (ModelState.IsValid)
			{
				var user = await _userManager.FindByNameAsync(model.Email);
				if (user == null || !(await _userManager.IsEmailConfirmedAsync(user)))
				{
					// Don't reveal that the user does not exist or is not confirmed
					return View("ForgotPasswordConfirmation");
				}

				// For more information on how to enable account confirmation and password reset please visit http://go.microsoft.com/fwlink/?LinkID=532713
				// Send an email with this link
				//var code = await _userManager.GeneratePasswordResetTokenAsync(user);
				//var callbackUrl = Url.Action("ResetPassword", "Account", new { userId = user.Id, code = code }, protocol: Context.Request.Scheme);
				//await _emailSender.SendEmailAsync(model.Email, "Reset Password",
				//   "Please reset your password by clicking here: <a href=\"" + callbackUrl + "\">link</a>");
				//return View("ForgotPasswordConfirmation");
			}

			// If we got this far, something failed, redisplay form
			return View(model);
		}

		//
		// GET: /Account/ForgotPasswordConfirmation
		[HttpGet]
		[AllowAnonymous]
		public IActionResult ForgotPasswordConfirmation()
		{
			return View();
		}

		//
		// GET: /Account/ResetPassword
		[HttpGet]
		[AllowAnonymous]
		public IActionResult ResetPassword(string code = null)
		{
			return code == null ? View("Error") : View();
		}

		//
		// POST: /Account/ResetPassword
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> ResetPassword(ResetPasswordViewModel model)
		{
			if (!ModelState.IsValid)
			{
				return View(model);
			}
			var user = await _userManager.FindByNameAsync(model.Email);
			if (user == null)
			{
				// Don't reveal that the user does not exist
				return RedirectToAction(nameof(ResetPasswordConfirmation), "Account");
			}
			var result = await _userManager.ResetPasswordAsync(user, model.Code, model.Password);
			if (result.Succeeded)
			{
				return RedirectToAction(nameof(ResetPasswordConfirmation), "Account");
			}
			AddErrors(result);
			return View();
		}

		//
		// GET: /Account/ResetPasswordConfirmation
		[HttpGet]
		[AllowAnonymous]
		public IActionResult ResetPasswordConfirmation()
		{
			return View();
		}

		//
		// GET: /Account/SendCode
		[HttpGet]
		[AllowAnonymous]
		public async Task<ActionResult> SendCode(string returnUrl = null, bool rememberMe = false)
		{
			var user = await _signInManager.GetTwoFactorAuthenticationUserAsync();
			if (user == null)
			{
				return View("Error");
			}
			var userFactors = await _userManager.GetValidTwoFactorProvidersAsync(user);
			var factorOptions = userFactors.Select(purpose => new SelectListItem { Text = purpose, Value = purpose }).ToList();
			return View(new SendCodeViewModel { Providers = factorOptions, ReturnUrl = returnUrl, RememberMe = rememberMe });
		}

		//
		// POST: /Account/SendCode
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> SendCode(SendCodeViewModel model)
		{
			if (!ModelState.IsValid)
			{
				return View();
			}

			var user = await _signInManager.GetTwoFactorAuthenticationUserAsync();
			if (user == null)
			{
				return View("Error");
			}

			// Generate the token and send it
			var code = await _userManager.GenerateTwoFactorTokenAsync(user, model.SelectedProvider);
			if (string.IsNullOrWhiteSpace(code))
			{
				return View("Error");
			}

			var message = "Your security code is: " + code;
			if (model.SelectedProvider == "Email")
			{
				await _emailSender.SendEmailAsync("", await _userManager.GetEmailAsync(user), "Security Code", message);
			}
			else if (model.SelectedProvider == "Phone")
			{
				await _smsSender.SendSmsAsync(await _userManager.GetPhoneNumberAsync(user), message);
			}

			return RedirectToAction(nameof(VerifyCode), new { Provider = model.SelectedProvider, model.ReturnUrl, model.RememberMe });
		}

		//
		// GET: /Account/VerifyCode
		[HttpGet]
		[AllowAnonymous]
		public async Task<IActionResult> VerifyCode(string provider, bool rememberMe, string returnUrl = null)
		{
			// Require that the user has already logged in via username/password or external login
			var user = await _signInManager.GetTwoFactorAuthenticationUserAsync();
			if (user == null)
			{
				return View("Error");
			}
			return View(new VerifyCodeViewModel { Provider = provider, ReturnUrl = returnUrl, RememberMe = rememberMe });
		}

		//
		// POST: /Account/VerifyCode
		[HttpPost]
		[AllowAnonymous]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> VerifyCode(VerifyCodeViewModel model)
		{
			if (!ModelState.IsValid)
			{
				return View(model);
			}

			// The following code protects for brute force attacks against the two factor codes.
			// If a user enters incorrect codes for a specified amount of time then the user account
			// will be locked out for a specified amount of time.
			var result = await _signInManager.TwoFactorSignInAsync(model.Provider, model.Code, model.RememberMe, model.RememberBrowser);
			if (result.Succeeded)
			{
				return RedirectToLocal(model.ReturnUrl);
			}
			if (result.IsLockedOut)
			{
				return View("Lockout");
			}
			ModelState.AddModelError("", "Invalid code.");
			return View(model);
		}

		#region Helpers

		// The following code creates the database and schema if they don't exist.
		// This is a temporary workaround since deploying database through EF migrations is
		// not yet supported in this release.
		// Please see this http://go.microsoft.com/fwlink/?LinkID=615859 for more information on how to do deploy the database
		// when publishing your application.
		private static void EnsureDatabaseCreated(ApplicationDbContext context)
		{
			if (!_databaseChecked)
			{
				_databaseChecked = true;
				context.Database.EnsureCreated();
			}
		}

		private void AddErrors(IdentityResult result)
		{
			foreach (var error in result.Errors)
			{
				ModelState.AddModelError(string.Empty, error.Description);
			}
		}

		private async Task<ApplicationUser> GetCurrentUserAsync()
		{
			return await _userManager.GetUserAsync(User);
		}

		private IActionResult RedirectToLocal(string returnUrl)
		{
			if (Url.IsLocalUrl(returnUrl))
			{
				return Redirect(returnUrl);
			}
			return RedirectToAction(nameof(HomeController.Index), "Home");
		}

		#endregion

		/// <summary>
		/// Verify validity of RegistrationCode.
		/// </summary>
		/// <param name="code"></param>
		/// <returns></returns>
		//[AllowAnonymous]
		//[HttpGet]
		//[Route("api/verifyRegistrationCode/{code}")]
		//public async Task<PreRegistrationModel> VerifyRegistrationCode(string code)
		//{
		//	var found = await _applicationDbContext.RegistrationCodes.FirstOrDefaultAsync(x => x.Code == code && !x.HasBeenUsed);
		//	if (found == null) Response.StatusCode = (int)HttpStatusCode.NotFound;
		//	return found != null
		//		? new PreRegistrationModel { IsSuccess = true, Code = code, Mail = found.Mail, Name = found.Name, Surname = found.Surname }
		//		: new PreRegistrationModel { IsSuccess = false };
		//}

		/// <summary>
		/// Registration API.
		/// </summary>
		/// <param name="model"></param>
		/// <returns></returns>
		[AllowAnonymous]
		[HttpPost, Produces("application/json")]
		[Route("api/registerWithCode")]
		public async Task<IActionResult> RegisterWithRegistrationCode([FromBody] RegisterViewModel model)
		{
			if (ModelState.IsValid)
			{
				//var availableCode = await _applicationDbContext.RegistrationCodes.FirstOrDefaultAsync(x => x.Code == model.RegistrationCode && !x.HasBeenUsed);
				//if (availableCode == null)
				//{
				//	ModelState.AddModelError(nameof(model.RegistrationCode), "Code is not valid or has been already used!");
				//	return Json(new HttpResponseMessageWithError(null, new List<string> { "Code is not valid or has been already used!" }));
				//}

				var user = new ApplicationUser { UserName = model.Email, Email = model.Email, DateOfAccident = model.DateOfAccident, Name = model.Name, Surname = model.Surname };
				var result = await _userManager.CreateAsync(user, model.Password);
				if (result.Succeeded)
				{
					var profileObjectKey = $"{Guid.NewGuid()}.jpg";

					if (!string.IsNullOrWhiteSpace(model.ProfileImage))
					{
						var bytes = Convert.FromBase64String(model.ProfileImage);
						var targetFilePath = Path.GetTempFileName();
						using (var imageFile = new FileStream(targetFilePath, FileMode.Create))
						{
							imageFile.Write(bytes, 0, bytes.Length);
							imageFile.Flush();
						}

						using (var amazonClient = new AmazonS3Helper(_amazonS3))
						{
							await amazonClient.UploadFileAsync(targetFilePath, profileObjectKey);
						}

						user.ProfilePictureKey = profileObjectKey;
					}

					// For more information on how to enable account confirmation and password reset please visit http://go.microsoft.com/fwlink/?LinkID=532713
					// Send an email with this link
					//var code = await _userManager.GenerateEmailConfirmationTokenAsync(user);
					//var callbackUrl = Url.Action("ConfirmEmail", "Account", new { userId = user.Id, code }, Request.Scheme);

					//TODO: cambiare con "Grazie per esserti registrato
					//await _emailSender.SendEmailAsync(model.Email, "Confirm your account", "Please confirm your account by clicking this link: <a href=\"" + callbackUrl + "\">link</a>");

					await _emailSender.SendEmailAsync(
						null,
						model.Email,
						"Welcome to Visual Evidence Recorder",
						"Welcome to Visual Evidence Recorder.",
						model.Name,
						model.Surname);

					//availableCode.HasBeenUsed = true;
					//availableCode.UserId = user.Id;

					await _applicationDbContext.SaveChangesAsync();

					//var ticket = await AuthorizationController.CreateTicketAsync(_signInManager, _identityOptions, new OpenIdConnectRequest { GrantType = "password", ClientId = "postman", Username = model.Email, Password = model.Password }, user);
					//var signInResult = SignIn(ticket.Principal, ticket.Properties, ticket.AuthenticationScheme);

					//return Json(signInResult);
					return Json(this.SetResponseAndReturn(statusCode: HttpStatusCode.Created));
				}

				return Json(this.SetResponseAndReturn(result.Errors.Select(x => $"{x.Code} - {x.Description}").ToList(), HttpStatusCode.BadRequest));
			}

			return Json(this.SetResponseAndReturn(statusCode: HttpStatusCode.BadRequest));
		}

		/// <summary>
		/// Password recovery.
		/// </summary>
		/// <param name="mail"></param>
		/// <returns></returns>
		[AllowAnonymous]
		[HttpGet]
		[Route("api/forgotPassword/{mail}")]
		public async Task<IActionResult> ForgotPasswordMobile(string mail)
		{
			var user = await _userManager.FindByNameAsync(mail);
			if (user == null) return NotFound();
			//var userDetail = await _applicationDbContext.RegistrationCodes.FirstOrDefaultAsync(x => x.UserId == user.Id);


			var code = await _userManager.GeneratePasswordResetTokenAsync(user);
			var callbackUrl = Url.Action("ResetPassword", "Account", new { userId = user.Id, code }, HttpContext.Request.Scheme);
			await _emailSender.SendEmailAsync(mail, "Reset Password", "Please reset your password by clicking here: <a href=\"" + callbackUrl + "\">link</a>", user.Name, user.Surname);

			return Json(new { result = true });
		}
	}
}