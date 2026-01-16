using EtudeManyToMany.API.Helpers;
using EtudeManyToMany.API.Repository;
using EtudeManyToMany.Core.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Runtime;
using System.Security.Claims;
using System.Text;

namespace EtudeManyToMany.API.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class AuthentificationController : ControllerBase
    {
        private readonly IRepository<Utilisateur> _utilisateurRepository;
        private readonly AppSettings _appSettings;

        public AuthentificationController(IRepository<Utilisateur> utilisateurRepository, IOptions<AppSettings> appSettings)
        {
            _utilisateurRepository = utilisateurRepository;
            _appSettings = appSettings.Value;
        }

        [HttpPost("login-url-encoded")]
        public async Task<IActionResult> LoginURL([FromForm] string email, [FromForm] string password)
        {
            string encryptedPassword = PasswordCrypter.EncryptPassword(password, _appSettings.SecretKey);

            var utilisateur = await _utilisateurRepository.Get(u => u.Email == email && u.Password == encryptedPassword);

            if (utilisateur == null)
                return BadRequest("Invalid Authentification");

            var role = utilisateur.isAdmin ? "Admin" : "User";

            List<Claim> claims = new List<Claim>()
            {
                new Claim(ClaimTypes.Role, Constants.RoleAdmin),
                new Claim(ClaimTypes.NameIdentifier, utilisateur.UtilisateurId.ToString()),
            };

            SigningCredentials signingCredentials = new SigningCredentials(
                new SymmetricSecurityKey(Encoding.ASCII.GetBytes(_appSettings.SecretKey!)),
                SecurityAlgorithms.HmacSha256);

            JwtSecurityToken jwt = new JwtSecurityToken(
               issuer: _appSettings.ValidIssuer,
               audience: _appSettings.ValidAudience,
               claims: claims,
               signingCredentials: signingCredentials,
               expires: DateTime.Now.AddDays(7)
               );

            var token = new JwtSecurityTokenHandler().WriteToken(jwt);

            return Ok(new
            {
                Token = token,
                Message = "Valid Authentication !",
                Utilisateur = utilisateur
            });
        }
    }
}
