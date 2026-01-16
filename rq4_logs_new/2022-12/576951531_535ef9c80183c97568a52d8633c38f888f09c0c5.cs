using EasyAgenda.Data.Contracts;
using EasyAgenda.Model;
using EasyAgendaBase.Model;
using EasyAgendaService;
using EasyAgendaService.Data.Contracts;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Data.SqlClient;

namespace EasyAgenda.Controllers
{
  [ApiController]
  [Route("/api")]
  public class HomeController : ControllerBase
  {
    private readonly IUserDAL _userRepository;
    private readonly ITokenService _tokenService;
    public HomeController(IUserDAL userRepository, ITokenService tokenService)
    {
      _userRepository = userRepository;
      _tokenService = tokenService;
    }
    [HttpPost, Route("v1/authenticate")]
    [AllowAnonymous]
    public async Task<IActionResult> Authenticate(Access access)
    {
      try
      {
        User user = await _userRepository.GetByEmail(access.Email);

        PasswordService decryptPassword = new(access.Password);

        if (!decryptPassword.VerifyHashPassword(user.Password))
          throw new UnauthorizedAccessException("wrong email or password.");

        user.Password = "";
        return Ok(new
        {
          user,
          token = _tokenService
          .GenerateToken(new UserRole(user.Id, user.Email, user.Password, user.Role.Description))
        });

      }
      catch (KeyNotFoundException error)
      {
        return NotFound(error);
      }
      catch(UnauthorizedAccessException error)
      {
        return BadRequest(error);
      }
      catch (SqlException error)
      {
        return StatusCode(StatusCodes.Status500InternalServerError, error.Message);
      }
      catch (Exception error)
      {
        return StatusCode(StatusCodes.Status500InternalServerError, error.Message);
      }
    }
  }
}