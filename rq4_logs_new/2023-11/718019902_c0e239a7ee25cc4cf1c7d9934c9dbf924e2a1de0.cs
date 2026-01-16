using Microsoft.AspNetCore.Mvc;
using gamer_store_api.Services;
using gamer_store_api.Data.Models;
using gamer_store_api.Data.DTOs;

namespace gamer_store_api.Controllers;

[ApiController]
[Route("[controller]")]
public class RolesController: ControllerBase
{
    private readonly RolesService _service;

    public RolesController(RolesService service)
    {
        _service = service;
    }

    #region get
    [HttpGet]
    public async Task<IEnumerable<Rol>> GetRoles()
    {
        return await _service.GetRoles();
    }

    [HttpGet("{idRol}")]
    public async Task<ActionResult<Rol>> GetRolById(int idRol)
    {
        var rol = await _service.GetRolById(idRol);

        if(rol is null){
            return RolNoEncontrado(idRol);
        }
        else{
            return rol;
        }
    }
    #endregion get

    #region set
    [HttpPost]
    public async Task<IActionResult> InsertRol(RolDtoSet rol){        
        var newRol = await _service.InsertRol(rol);  

        return CreatedAtAction(nameof(GetRolById), new { idRol = newRol.IdRol }, newRol);
    }
    #endregion set

    #region put
    [HttpPut("{idRol}")]
    public async Task<IActionResult> UpdateRol(int idRol, RolDtoSet rol){
        if(idRol != rol.IdRol) return BadRequest(new { message = $"No coinciden los ids proporcionados" });

        var rolUpdated = await _service.UpdateRol(idRol, rol);

        if(rolUpdated is not null) return CreatedAtAction(nameof(GetRolById), new { idRol = rolUpdated.IdRol }, rolUpdated);

        return RolNoEncontrado(idRol);
    }

    [HttpPut("delete/{idRol}")]
    public async Task<IActionResult> DeleteRol(int idRol){
        var rolDeleted = await _service.DeleteRol(idRol);
        if(rolDeleted is true) return Ok();

        return RolNoEncontrado(idRol);
    }
    #endregion put

    public NotFoundObjectResult RolNoEncontrado(int idRol){
        return NotFound(new { message = $"No existe el rol con id = {idRol}." });
    }
}