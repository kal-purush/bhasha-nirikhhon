using comp1640_dotnet.Data;
using comp1640_dotnet.DTOs.Requests;
using comp1640_dotnet.DTOs.Responses;
using comp1640_dotnet.Models;
using comp1640_dotnet.Repositories.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace comp1640_dotnet.Controllers
{
	[Route("[controller]")]
	[ApiController]
	[Authorize(Roles = "Administrator")]
	public class RolesController : ControllerBase
	{
		private readonly IRoleRepository _roleRepos;

		public RolesController(IRoleRepository roleRepos)
		{
			_roleRepos = roleRepos;
		}

		[HttpGet]
		public async Task<ActionResult<UserRole>> GetRoles()
		{
			var result = await _roleRepos.GetRoles();

			if(result == null)
			{
				return BadRequest("Roles not found");
			}
			return Ok(result);
		}
	}
}