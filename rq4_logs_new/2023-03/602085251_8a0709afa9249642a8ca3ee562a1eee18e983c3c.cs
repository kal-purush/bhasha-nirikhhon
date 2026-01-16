
using comp1640_dotnet.DTOs.Responses;
using comp1640_dotnet.Models;
using comp1640_dotnet.Repositories.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace comp1640_dotnet.Controllers
{
	[Route("[controller]")]
	[ApiController]
	[Authorize(Roles = "Administrator")]
	public class UserController : ControllerBase
	{
		private readonly IUserRepository _userRepos;

		public UserController(IUserRepository userRepos)
		{
			_userRepos = userRepos;
		}

		[HttpGet]
		public async Task<ActionResult<List<UserResponse>>> GetIdleUsers()
		{
			var result = await _userRepos.GetIdleUsers();

			if(result == null)
			{
				return BadRequest("Users not found");
			}
			return Ok(result);
		}
	}
}