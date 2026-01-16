using dotnet9_ketnoigiaothuong.Infrastructure.Context;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using dotnet9_ketnoigiaothuong.Domain.Entities;
using dotnet9_ketnoigiaothuong.Services.Token;
using dotnet9_ketnoigiaothuong.ViewModels.Auth;
using Microsoft.EntityFrameworkCore;
using System.Security.Claims;

namespace dotnet9_ketnoigiaothuong.Controllers
{
    [Route("api/auth/")]
    [ApiController]
    public class AuthController : ControllerBase
    {
        private readonly AppDbContext _context;
        private readonly ITokenService _tokenService;

        public AuthController(AppDbContext context, ITokenService tokenService)
        {
            _context = context;
            _tokenService = tokenService;
        }

        [HttpPost("register")]
        public async Task<IActionResult> Register(RegisterViewModel registerViewModel)
        {
            if (_context.UserAccounts.Any(u => u.Email == registerViewModel.Email))
                return BadRequest("Email already exists");

            var user = new UserAccount
            {
                FullName = registerViewModel.FullName,
                Email = registerViewModel.Email,
                PasswordHash = BCrypt.Net.BCrypt.HashPassword(registerViewModel.Password),
                Role = "Company",
                Status = null
            };

            _context.UserAccounts.Add(user);
            await _context.SaveChangesAsync();
            return Ok("User registered");
        }

        [HttpPost("login")]
        public async Task<IActionResult> Login([FromBody] LoginViewModel loginViewModel)
        {
            var user = await _context.UserAccounts.FirstOrDefaultAsync(u => u.Email == loginViewModel.Email);
            if (user == null || !BCrypt.Net.BCrypt.Verify(loginViewModel.Password, user.PasswordHash))
                return Unauthorized("Invalid credentials");

            var token = _tokenService.GenerateJwtToken(user);

            Response.Cookies.Append("jwt", token, new CookieOptions
            {
                HttpOnly = true,
                Secure = true,
            });

            return Ok(new { message = "Logged in successfully" });
        }

        [HttpPost("logout")]
        public IActionResult Logout()
        {
            Response.Cookies.Delete("jwt");
            return Ok(new { message = "Logged out" });
        }

        [Authorize]
        [HttpGet("me")]
        public async Task<IActionResult> Me()
        {
            //var username = User.Identity.Name;
            var email = User.FindFirst(ClaimTypes.Email)?.Value;
            var user = await _context.UserAccounts.FirstOrDefaultAsync(u => u.Email == email);
            return Ok(user);
        }
    }
}