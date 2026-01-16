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
	[Authorize]
	public class ProfilesController : ControllerBase
	{
		private readonly IProfileRepository _profileRepos;

		public ProfilesController(IProfileRepository profileRepos)
		{
			_profileRepos = profileRepos;
		}

		[HttpGet]
		public async Task<ActionResult<ProfileResponse>> GetProfile()
		{
			var result = await _profileRepos.GetProfile();
			if(result == null)
			{
				return BadRequest("Profile not found");
			}
			return Ok(result);
		}

		[HttpPut("{id}")]
		public async Task<ActionResult<ProfileResponse>> UpdateProfile(string id, ProfileRequest profile)
		{
			var result = await _profileRepos.UpdateProfile(id, profile);
			if (result == null)
			{
				return BadRequest("Profile not found");
			}
			return Ok("Update successful profile");
		}
	}
}