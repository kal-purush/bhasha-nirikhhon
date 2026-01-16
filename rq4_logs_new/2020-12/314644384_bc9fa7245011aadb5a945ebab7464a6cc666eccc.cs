using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using PetOwner.DTOs;
using PetOwner.Mappers;
using PetOwner.Models;
using PetOwner.Repository.Implementations;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace PetOwner.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class UserController : ControllerBase
	{
		private readonly UserRepository _userRepository;
		private readonly GroupRepository _groupRepository;
		public UserController(UserRepository userRepository, GroupRepository groupRepository)
		{
			_userRepository = userRepository;
		}

		// GET: api/<UserController>
		[HttpPost("login")]
		public ActionResult<int> Login(UserLoginRequest userLogin)
		{
			var user = _userRepository.Login(userLogin);
			return Ok(user.UserId);
		}

		[HttpPost("register")]
		public ActionResult<int> Register(UserRegisterRequest userRegister)
		{
			var userCreate = userRegister.ToUser();
			_userRepository.Insert(userCreate);
			var user =_userRepository.GetByEmailAndPassword(userCreate.Email, userCreate.Password);
			//TO BE CODED
			//_groupRepository.AttachUser(user);

			return Ok(user.UserId);
		}

		// GET api/<UserController>/5
		[HttpGet("{id}")]
		public ActionResult<User> Get(int id)
		{
			return _userRepository.Get(id);
		}

		// PUT api/<UserController>/5
		[HttpPut("{id}")]
		public void Put(int id, [FromBody] string value)
		{
		}

		// DELETE api/<UserController>/5
		[HttpDelete("{id}")]
		public ActionResult Delete(int id)
		{
			_userRepository.Delete(_userRepository.Get(id));
			return Ok(_userRepository.Save());
		}
	}
}