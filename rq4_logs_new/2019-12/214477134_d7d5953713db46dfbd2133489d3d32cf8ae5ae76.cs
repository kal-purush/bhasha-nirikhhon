using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using CentralDeErros.Application.Interfaces;
using CentralDeErros.Application.ViewModel;
using CentralDeErros.Application.ViewModel.Authentication;
using CentralDeErros.CrossCutting.Helpers;
using CentralDeErros.CrossCutting.Utils;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace CentralDeErros.Api.Controllers
{
    [Route("api/login")]
    [ApiController]
    public class AuthenticationController : ControllerBase
    {
        private readonly AppSettings _appSettings;
        private readonly IUserAppService _userAppService;

        public AuthenticationController(IUserAppService userAppService, IOptions<AppSettings> appSettings)
        {
            _userAppService = userAppService;
            _appSettings = appSettings.Value;
        }
        [AllowAnonymous]
        [HttpPost()]
        public ActionResult Post(LoginViewModel loginViewModel)
        {
            var user = _userAppService.Find(p => p.Login == loginViewModel.Login).FirstOrDefault();

            if (user == null)
                return BadRequest("Usuário não cadastrado.");

            if (user.Password != loginViewModel.Password.ToHashMD5())
                return BadRequest("Senha inválida.");

            if (!user.Active)
                return BadRequest("Usuário inativo.");

            user.AccessToken = GenerateJWT(user);




            return Ok(user);
        }

        private string GenerateJWT(UserViewModel usuario)
        {
            var tokenHandler = new JwtSecurityTokenHandler();
            var key = Encoding.ASCII.GetBytes(_appSettings.SecretKeyJWT);

            var tokenDescriptor = new SecurityTokenDescriptor
            {
                Subject = new ClaimsIdentity(new Claim[]
                {
                    new Claim(ClaimTypes.Name, usuario.Name),
                    new Claim(ClaimTypes.Email, usuario.Email),
                    new Claim("id", usuario.Id.ToString()),
                }),

                Expires = DateTime.UtcNow.AddMinutes(30),

                SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256Signature)
            };

            var token = tokenHandler.CreateToken(tokenDescriptor);
            return tokenHandler.WriteToken(token);
        }


    }
}