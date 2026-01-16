using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using home_swap_api.Dto;
using home_swap_api.Models;
using home_swap_api.Service;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;

namespace home_swap_api.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class AuthController : ControllerBase
	{
	

        private readonly UserService userService;
        private readonly IConfiguration configuration;

        public AuthController(UserService userService, IConfiguration configuration)
        {
            this.userService = userService;
            this.configuration = configuration;
        }

        [HttpPost("register")]
        public async Task<ActionResult<User>> Register([FromBody] UserDTO userDTO)
        {
            var user = new User();
            var existingUser = await userService.GetUserByUsername(userDTO.Username);
            if (existingUser is not null)
                return BadRequest("username already taken");
            string passwordHash = BCrypt.Net.BCrypt.HashPassword(userDTO.Password);

			user.Username = userDTO.Username;
			user.PasswordHash = passwordHash;
			var result = await userService.AddUser(user);

            string token = CreateToken(result);

            return Ok(token);
        }

        [HttpPost("login")]
        public async Task<ActionResult<User>> Login([FromBody] UserDTO userDTO)
        {
            var result = await userService.GetUserByUsername(userDTO.Username);
            if (result is null)
                return BadRequest("user not found");

            if (!BCrypt.Net.BCrypt.Verify(userDTO.Password, result.PasswordHash))
            {
                return BadRequest("wrong password");
            }

            string token = CreateToken(result);

            return Ok(token);
        }

        private string CreateToken(User user)
        {
            List<Claim> claims = new List<Claim>
            {
                new Claim(ClaimTypes.Name,user.Username)
            };

            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(
                configuration.GetSection("AppSettings:Token").Value!));

            var cred = new SigningCredentials(key, SecurityAlgorithms.HmacSha512Signature);

            var token = new JwtSecurityToken(
                    claims: claims,
                    expires: DateTime.Now.AddDays(1),
                    signingCredentials: cred
                );

            var jwt = new JwtSecurityTokenHandler().WriteToken(token);

            return jwt;
        }


    }
}
