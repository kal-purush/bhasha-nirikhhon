using ApiManager;
using AuthProvider;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System.IdentityModel.Tokens.Jwt;
using System.Threading.Tasks;


namespace DMG.Web.Api
{
    [Route("[controller]/[action]")]
    public class AuthController : Controller
    {
        private readonly IConfiguration _config;
        private readonly IAuthenticationProvider _auth;
        private readonly IUserService _srv;

        public AuthController(IUserService srv, IConfiguration config, IAuthenticationProvider auth)
        {
            _srv = srv;
            _config = config;
            _auth = auth;
        }

        [HttpPost]
        public async Task<IActionResult> Token([FromBody] LoginViewModel model)
        {
            if (ModelState.IsValid)
            {
                var dbUser = await _srv.GetByUsername(model.Username);

                if (dbUser.FirstLogin)
                {
                    if (dbUser.Password == model.Password)
                    {
                        var verificationToken = _srv.FirstLoginProcess(dbUser).VerificationToken;
                        if (!string.IsNullOrEmpty(verificationToken))
                        {
                            return Ok("firstlogin");
                        }
                        return BadRequest();
                    }
                    return BadRequest("Λάθος όνομα χρήστη ή κωδικός");

                }

                if (dbUser != null && PasswordHasher.VerifyHashedPassword(dbUser.Password, model.Password))
                {
                    var userToken = _auth.CreateToken(dbUser, _config);

                    return Ok(new { token = new JwtSecurityTokenHandler().WriteToken(userToken) });
                }
                else
                {
                    return BadRequest("Λάθος όνομα χρήστη ή κωδικός");
                }
            }

            return BadRequest("Σφάλμα δημιουργίας κλειδιού ασφαλείας.");
        }
    }
}