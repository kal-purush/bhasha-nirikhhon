using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Neiria.Domain.Interfaces;
using Neiria.Domain.Models;
using Neiria.Domain.ViewModels;

namespace Neiria.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {
    private readonly IUserRepo _repo;
    private readonly IMapper _mapper;

    public UserController(IUserRepo repo, IMapper mapper)
    {
      _repo = repo;
      _mapper = mapper;
    }

    [HttpGet]
    [ProducesResponseType(typeof(IEnumerable<UserViewModel>), (int)HttpStatusCode.OK)]
    public async Task<ActionResult<IEnumerable<UserViewModel>>> GetClothes()
    {
      try
      {
        var users = await _repo.GetAll();
        var mapUsers = _mapper.Map<IEnumerable<UserViewModel>>(users);
        return Ok(mapUsers);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpGet("{id}")]
    [ProducesResponseType(typeof(UserViewModel), (int)HttpStatusCode.OK)]
    public async Task<ActionResult<UserViewModel>> GetSpecificCloth(Guid id)
    {
      try
      {
        var result = await _repo.GetId(id);
        var mapResult = _mapper.Map<UserViewModel>(result);
        return Ok(mapResult);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpPost]
    [ProducesResponseType(typeof(UserViewModel), (int)HttpStatusCode.Created)]
    public async Task<IActionResult> CreateNewCloth([FromBody] UserViewModel user)
    {
      try
      {
        var result = await _repo.Insert(_mapper.Map<User>(user));
        var mapResult = _mapper.Map<UserViewModel>(result);
        return StatusCode((int)HttpStatusCode.Created, mapResult);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpPut("{id}")]
    [ProducesResponseType(typeof(UserViewModel), (int)HttpStatusCode.OK)]
    public async Task<IActionResult> UpdateCloth(Guid id, [FromBody] UserViewModel user)
    {
      try
      {
        var result = await _repo.Update(id, _mapper.Map<User>(user));
        var mapResult = _mapper.Map<UserViewModel>(result);
        return StatusCode((int)HttpStatusCode.Created, mapResult);
      }
      catch (Exception ex)
      {
        return StatusCode(500, ex.Message);
      }
    }

    [HttpDelete("{id}")]
    [ProducesResponseType((int)HttpStatusCode.NoContent)]
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
  