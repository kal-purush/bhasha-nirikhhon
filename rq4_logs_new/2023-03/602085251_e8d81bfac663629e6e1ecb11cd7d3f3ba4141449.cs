using comp1640_dotnet.Data;
using comp1640_dotnet.DTOs.Requests;
using comp1640_dotnet.DTOs.Responses;
using comp1640_dotnet.Models;
using comp1640_dotnet.Repositories;
using comp1640_dotnet.Repositories.Interfaces;
using comp1640_dotnet.Services.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Org.BouncyCastle.Crypto;

namespace comp1640_dotnet.Controllers
{
	[Route("[controller]")]
	[ApiController]
	[Authorize (Roles = "Quality Assurance Coordinator")]
	public class InvitationsController : ControllerBase
	{
		private readonly IEmailService _emailService;
		private readonly IUserRepository _userRepos;

		public InvitationsController(IEmailService emailService, IUserRepository userRepos)
		{
			_emailService = emailService;
			_userRepos = userRepos;
		}

		[HttpPost]
		public async Task<ActionResult<CommentResponse>> CreateInvitation(string inviteUserId)
		{
			User user = await _userRepos.GetUser(inviteUserId);
			if(user == null)
			{
				return BadRequest("User not found");
			}
		  _emailService.SendEmail(user.Email, "QA coordinator has just invited you to generate ideas", "< a href = '' > Click </ a > ");

			return Ok();
		}
	}
}