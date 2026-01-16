using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Neiria.Domain.Interfaces;
using Neiria.Domain.Models;

namespace Neiria.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CatergoryController : ControllerBase
    {
    private readonly ICatergoryRepo _repo;

    public CatergoryController(ICatergoryRepo repo)
    {
      _repo = repo;
    }

    [HttpGet]
    [ProducesResponseType(typeof(IEnumerable<Catergory>), (int)HttpStatusCode.OK)]
    public async Task<ActionResult<IEnumerable<Catergory>>> GetClothes()
    {
      try
      {
        var items = await _repo.GetAll();
        return Ok(items);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpPost]
    [ProducesResponseType(typeof(Catergory), (int)HttpStatusCode.Created)]
    public async Task<IActionResult> CreateNewCloth([FromBody] Catergory cloth)
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
  }
}