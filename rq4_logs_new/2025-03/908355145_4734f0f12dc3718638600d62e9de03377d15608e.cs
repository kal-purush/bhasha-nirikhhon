using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using ApiCatalogo.DTOs.LoginModel;
using ApiCatalogo.DTOs.RegistroModel;
using ApiCatalogo.DTOs.Response;
using ApiCatalogo.DTOs.TokenModel;
using ApiCatalogo.Models.ApplicationUser;
using ApiCatalogo.Services.TokenService;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;

namespace ApiCatalogo.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AuthController : ControllerBase
    {
        private readonly ITokenService _tokenService;
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly RoleManager<IdentityRole> _roleManager;
        private readonly IConfiguration _configuration;


        public AuthController(ITokenService tokenService, UserManager<ApplicationUser> userManager, RoleManager<IdentityRole> roleManager, IConfiguration configuration)
        {
            _tokenService = tokenService;
            _userManager = userManager;
            _roleManager = roleManager;
            _configuration = configuration;
        }


        [HttpPost]
        [Route("login")]
        public async Task<IActionResult> Login([FromBody] LoginModel model)
        {
            var usuario = await _userManager.FindByNameAsync(model.NomeUsuario!);

            if (usuario is not null && await _userManager.CheckPasswordAsync(usuario, model.Senha!))
            {

                var userRoles = await _userManager.GetRolesAsync(usuario);

                var authClaims = new List<Claim>{
                    new Claim(ClaimTypes.Name, usuario.UserName!),
                    new Claim(ClaimTypes.Email, usuario.Email!),
                    new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString())
                };

                foreach (var roles in userRoles)
                {
                    authClaims.Add(new Claim(ClaimTypes.Role, roles));

                }
                var token = _tokenService.GenerateAccessToken(authClaims);
                var refreshToken = _tokenService.GenerateRefreshToken();

                _ = int.TryParse(_configuration["JWT:RefreshTokenValidityInMinutes"], out int refreshTokenValidityInMinutes);

                usuario.RefreshTokenExpiryTime = DateTime.Now.AddMinutes(refreshTokenValidityInMinutes);

                usuario.RefreshToken = refreshToken;

                await _userManager.UpdateAsync(usuario);

                return Ok(new
                {
                    Token = new JwtSecurityTokenHandler().WriteToken(token),
                    RefreshToken = refreshToken,
                    Expiration = token.ValidTo,

                });

            }

            return Unauthorized();

        }

        [HttpPost]
        [Route("register")]
        public async Task<IActionResult> RegistrarUsuario([FromBody] RegistroModel model)
        {

            var usuarioExiste = await _userManager.FindByEmailAsync(model.NomeUsuario!);

            if (usuarioExiste is not null)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, new Response { Status = "Error", Mensagem = "Usuário já existe!" });
            }

            ApplicationUser usuario = new()
            {
                Email = model.Email,
                SecurityStamp = Guid.NewGuid().ToString(),
                UserName = model.NomeUsuario
            };

            var resultado = await _userManager.CreateAsync(usuario, model.Senha!);

            if (!resultado.Succeeded)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, new Response { Status = "Error", Mensagem = "Usuário não foi criado!" });
            }

            return Ok(new Response { Status = "Success", Mensagem = "Usuário criado com sucesso!" });

        }

        [HttpPost]
        [Route("refresh-token")]

        public async Task<IActionResult> RefreshToken(TokenModel tokenModel)
        {
            if (tokenModel is null)
            {
                return BadRequest("Resposta do cliente inválida");
            }

            string? acessoToken = tokenModel.AcessoToken ?? throw new ArgumentNullException(nameof(tokenModel));
            string? tokenAtualizar = tokenModel.AtualizarToken ?? throw new ArgumentNullException(nameof(tokenModel));

            var principal = _tokenService.GetPrincipalFromExpiredToken(acessoToken!);

            if (principal is null)
            {
                return BadRequest("Acesso ao token inválido, Não pode atualizar");
            }
            string nomeUsuario = principal.Identity.Name;

            var usuario = await _userManager.FindByNameAsync(nomeUsuario!);

            if (usuario is null || usuario.RefreshToken != tokenAtualizar || usuario.RefreshTokenExpiryTime <= DateTime.Now)
            {
                return BadRequest("Acesso ao token inválido, Não pode atualizar");

            }

            var novoAcessoToken = _tokenService.GenerateAccessToken(principal.Claims.ToList());

            var novoTokenAtualizado = _tokenService.GenerateRefreshToken();

            usuario.RefreshToken = novoTokenAtualizado;

            await _userManager.UpdateAsync(usuario);

            return new ObjectResult(new
            {
                acessoToken = new JwtSecurityTokenHandler().WriteToken(novoAcessoToken),
                tokenAtualizar = novoAcessoToken
            });

        }

        [Authorize]
        [HttpPost]
        [Route("revogar/{nomeUsuario}")]
        public async Task<IActionResult> Revogar(string nomeUsuario)
        {

            var authenticatedUser = await _userManager.GetUserAsync(User);

            if (authenticatedUser == null)
            {
                return Unauthorized();
            }

            // Verifique se o usuário autenticado é o mesmo que está tentando revogar o token
            bool isSelfRevoking = authenticatedUser.UserName == nomeUsuario;

            // Verifique se o usuário autenticado é um Super Admin
            bool isSuperAdmin = await _userManager.IsInRoleAsync(authenticatedUser, "SuperAdmin");

            // Permita a revogação apenas se o usuário estiver revogando seu próprio token ou se for um Super Admin
            if (!isSelfRevoking && !isSuperAdmin)
            {
                return Forbid();
            }
            var usuario = await _userManager.FindByNameAsync(nomeUsuario);

            if (usuario is null)
            {
                return NotFound(new { Mensagem = "Usuário não encontrado!" });
            }

            usuario.RefreshToken = null;

            await _userManager.UpdateAsync(usuario);

            return Ok(new { Mensagem = "A sessão do usuário foi revogada com sucesso!" });
        }


    }
}