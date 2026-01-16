using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using PetOwner.Data;
using PetOwner.DTOs;
using PetOwner.Mappers;
using PetOwner.Models;
using PetOwner.Repository.Interfaces;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace PetOwner.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class GroupController : ControllerBase
	{
		private readonly IGroupRepository _groupRepository;
		private readonly IUserRepository _userRepository;
		private readonly PetOwnerContext _context;

		public GroupController(IGroupRepository groupRepository, IUserRepository userRepository, PetOwnerContext context)
		{
			_groupRepository = groupRepository;
			_userRepository = userRepository;
			_context = context;
		}
		// GET: api/<GroupController>
		[HttpGet]
		public IEnumerable<string> Get()
		{
			return new string[] { "value1", "value2" };
		}

		// GET api/<GroupController>/5
		[HttpGet("{id}")]
		public ActionResult<Group> Get(int id)
		{
			var group = _groupRepository.Get(id);

			if(group != null)
			{
				return Ok(group);
			}
			return BadRequest();
		}

		[HttpGet("{id}/users")]
		public ActionResult<List<UserProfileDTO>> GetUsers(int id)
		{
			var users = _context.Users.Where(x => x.GroupId == id).ToList();
			
			if(users == null)
			{
				return BadRequest();
			}

			List<UserProfileDTO> result = new List<UserProfileDTO>();

			foreach(User user in users)
			{
				var userLevelVip = _userRepository.GetUserWithLevelVip(id);

				result.Add(userLevelVip.ToProfile());
			}

			return Ok(result);
		}

		// POST api/<GroupController>
		[HttpPost]
		public void Post([FromBody] string value)
		{
		}

		// PUT api/<GroupController>/5
		[HttpPut("{id}")]
		public void Put(int id, [FromBody] string value)
		{
		}

		// DELETE api/<GroupController>/5
		[HttpDelete("{id}")]
		public void Delete(int id)
		{
		}
	}
}