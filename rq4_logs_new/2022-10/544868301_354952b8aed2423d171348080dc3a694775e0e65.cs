using CropManagementAPI.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using System;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;

namespace CropManagementAPI.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class LoginController : ControllerBase
	{
		private IConfiguration _config;
		public LoginController(IConfiguration config)
		{
			_config = config;
		}
		
		[AllowAnonymous]
		[HttpPost]

		public IActionResult Login([FromBody] UserLogin userLogin)
		{
			var user = Authenticate(userLogin);
			
			if (user != null)
			{
				var token = GenerateJSONWebToken(user);
				return Ok(token);
			}

			return NotFound("Invalid username or password");
		}
		
		private string GenerateJSONWebToken(UserModel user)
		{
			var securityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_config["Jwt:Key"]));
			var credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);

			var claims = new[]
			{
				new Claim(ClaimTypes.NameIdentifier, user.Username),
				new Claim(ClaimTypes.Email, user.EmailAddress),
				new Claim(ClaimTypes.Role, user.Role),
				new Claim(ClaimTypes.Name, user.DisplayName),
				new Claim(ClaimTypes.UserData, user.Bio),
				new Claim(ClaimTypes.Actor, user.FavouriteCrops.ToString())
			};

			var token = new JwtSecurityToken(_config["Jwt:Issuer"],
				_config["Jwt:Audience"],
				claims,
				expires: DateTime.Now.AddMinutes(15),
				signingCredentials: credentials);

			return new JwtSecurityTokenHandler().WriteToken(token);
		}

		private UserModel Authenticate(UserLogin userLogin)
		{
			var currentUser = UserConstants.Users.FirstOrDefault(x => x.Username.ToLower() == userLogin.Username.ToLower() && x.Password == userLogin.Password);

			if (currentUser != null)
			{
				return currentUser;
			}

			return null;
		}
	}
}