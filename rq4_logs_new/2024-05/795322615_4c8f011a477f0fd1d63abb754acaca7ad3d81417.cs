using api.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using api.Dtos.Usuario;

namespace api.Controllers
{
  [ApiController]
  [Route("api/usuario")]
  public class UsuariosController : ControllerBase
  {
    private readonly UserManager<Usuarios> _userManager;

    public UsuariosController(UserManager<Usuarios> userManager)
    {
      _userManager = userManager;
    }

    [HttpPost("register")]
    public async Task<ActionResult> CrearUsuario([FromBody] CreateUsuarioDto CreateUsuarioDto)
    {
      try{
        if(!ModelState.IsValid)
        return BadRequest(ModelState);

        var usuario = new Usuarios
        {
          UserName = CreateUsuarioDto.UserName,
          Email = CreateUsuarioDto.Email,
        };

        var result = await _userManager.CreateAsync(usuario, CreateUsuarioDto.Password);

        if (result.Succeeded)
        {
          var roleResult = await _userManager.AddToRoleAsync(usuario, "User");
          if (roleResult.Succeeded)
          {
            return Ok("Usuario Creado");
          }
          else
          {
            return StatusCode(500, roleResult.Errors);
          }
        }
        else
        {
          return StatusCode(500, result.Errors);
        }

      } catch (Exception e){
        return StatusCode(500, e);
      }

    }
/*
    [HttpGet("{id}")]
    public async Task<ActionResult<CreateUsuarioDto>> ObtenerUsuario(string id)
    {
      var usuario = await _userManager.FindByIdAsync(id);

      if (usuario == null)
      {
        return NotFound();
      }

      // Mapear el usuario a UsuarioDto
      var usuarioDto = new UsuarioDto
      {
        Id = usuario.Id,
        UserName = usuario.UserName,
        // Mapear otras propiedades si es necesario
      };

      return Ok(usuarioDto);
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> ActualizarUsuario(string id, UsuarioDto usuarioDto)
    {
      if (id != usuarioDto.Id)
      {
        return BadRequest();
      }

      var usuario = await _userManager.FindByIdAsync(id);

      if (usuario == null)
      {
        return NotFound();
      }

      usuario.UserName = usuarioDto.UserName;
      // Actualizar otras propiedades si es necesario

      var result = await _userManager.UpdateAsync(usuario);

      if (result.Succeeded)
      {
        return NoContent();
      }
      else
      {
        return BadRequest(result.Errors);
      }
    }

    [HttpDelete("{id}")]
    public async Task<IActionResult> EliminarUsuario(string id)
    {
      var usuario = await _userManager.FindByIdAsync(id);

      if (usuario == null)
      {
        return NotFound();
      }

      var result = await _userManager.DeleteAsync(usuario);

      if (result.Succeeded)
      {
        return NoContent();
      }
      else
      {
        return BadRequest(result.Errors);
      }
    }*/
  }
}