using CropManagementAPI.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Linq;
using System.Security.Claims;

namespace CropManagementAPI.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class UserController : ControllerBase
	{
		[HttpGet("Admin")]
		[Authorize]
		
		public IActionResult AdminEndpoint()
		{
			var currentUser = GetCurrentUser();

			return Ok($"Hi {currentUser.DisplayName}, you are an {currentUser.Role}!");
		}

		[HttpGet("Public")]
		public IActionResult GetPublic()
		{
			return Ok("This is a public endpoint");
		}
		
		private UserModel GetCurrentUser()
		{
			var identity = HttpContext.User.Identity as ClaimsIdentity;

			if (identity != null)
			{
				var userClaims = identity.Claims;

				return new UserModel
				{
					Username = userClaims.FirstOrDefault(c => c.Type == ClaimTypes.Name)?.Value,
					EmailAddress = userClaims.FirstOrDefault(c => c.Type == ClaimTypes.Email)?.Value,
					Role = userClaims.FirstOrDefault(c => c.Type == ClaimTypes.Role)?.Value,
					DisplayName = userClaims.FirstOrDefault(c => c.Type == ClaimTypes.Name)?.Value,
					Bio = userClaims.FirstOrDefault(c => c.Type == ClaimTypes.UserData)?.Value,
					FavouriteCrops = userClaims.FirstOrDefault(c => c.Type == ClaimTypes.UserData)?.Value.Split(',')
				};
			}
			return null;
		}
	}
}