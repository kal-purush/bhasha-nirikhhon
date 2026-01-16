using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureAdB2cWebApp.Controllers {
	[Authorize]
	public class AccountController : Controller {
		[AllowAnonymous]
		public IActionResult Login() {
			return Content($"{nameof(Login)}");
		}
	}
}