using Application.IServices;
using Data.Dtos;
using Data.Models;
using Data.ViewModels;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;

namespace Application.Services
{
    public class AuthService : IAuthServeice
    {
        private readonly DrugRepositoryDBContext _context;
        public AuthService(DrugRepositoryDBContext context)
        {
            _context = context;
        }

        public Task<LoginView> Login(LoginDto request)
        {
            if(request == null)
            {
                return Task.FromResult(new LoginView() { userId = "", token = "" });
            }
            try
            {
                var res = _context.Database.ExecuteSqlCommand("USP_CHECK_LOGIN @userId, @userPass", parameters: request).ToString();
                if(res == "FALSE")
                {
                    return Task.FromResult(new LoginView() { userId = "", token = "" });
                }
                else
                {
                    var claims = new[]
                {
                    new Claim(JwtRegisteredClaimNames.Sub,_configuration["Jwt:Subject"]),
                    new Claim(JwtRegisteredClaimNames.Jti,Guid.NewGuid().ToString()),
                    new Claim(JwtRegisteredClaimNames.Iat, DateTime.UtcNow.ToString()),
                    new Claim("username",_user.username),
                };
                    var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration["Jwt:Key"]));
                    var signIn = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
                    var token = new JwtSecurityToken(_configuration["Jwt:Issuer"], _configuration["Jwt:Audience"], claims, expires: DateTime.UtcNow.AddDays(1), signingCredentials: signIn);
                    LoginView result = new LoginView();
                    {
                        result.username = _user.username;
                        result.token = new JwtSecurityTokenHandler().WriteToken(token);
                    };
                    return result;
                }
            }catch(Exception ex)
            {
                throw ex;
            }
        }
    }
}