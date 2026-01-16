using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Neiria.Domain.Interfaces;
using Neiria.Domain.Models;
using Neiria.Infrastructure.Repositories;

namespace Neiria.Api.Controllers
{
  [Route("api/[controller]")]
  [ApiController]
  public class ClothController : ControllerBase
  {
    private readonly IClothRepo _repo;

    public ClothController(ClothRepo repo)
    {
      this._repo = repo;
    }

    [HttpGet]
    [ProducesResponseType(typeof(IEnumerable<Cloth>),(int) HttpStatusCode.OK)]
    public async Task<ActionResult<IEnumerable<Cloth>>> GetClothes()
    {
      try 
      {
        var items = await _repo.GetAll();
        return Ok(items);
      }
      catch (Exception ex) {
        return StatusCode(500, ex.Message);
      }

    }

    [HttpGet("{id}")]
    [ProducesResponseType(typeof(Cloth), (int)HttpStatusCode.OK)]
    public async Task<ActionResult<Cloth>> GetSpecificCloth(Guid id)
    {
      try
      {
        var result = await _repo.GetId(id);
        return Ok(result);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpPost]
    [ProducesResponseType(typeof(Cloth), (int)HttpStatusCode.Created)]
    public async Task<IActionResult> CreateNewCloth([FromBody] Cloth cloth) 
    {
      try
      {
        var result = await _repo.Insert(cloth);
        return StatusCode((int)HttpStatusCode.Created, result);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpPut]
    [ProducesResponseType(typeof(Cloth), (int)HttpStatusCode.OK)]
    public async Task<IActionResult> UpdateCloth(Guid id, [FromBody] Cloth cloth)
    {
      try
      {
        var result = await _repo.Update(id ,cloth);
        return StatusCode((int)HttpStatusCode.Created, result);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpDelete("{id}")]
    [ProducesResponseType(typeof(Cloth), (int)HttpStatusCode.NoContent)]
    public async Task<ActionResult> DeleteCloth(Guid id)
    {
      try
      {
        await _repo.Delete(id);

        return NoContent();
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }
  }
}