using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using PetOwner.Helpers;
using PetOwner.Repository.Interfaces;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace PetOwner.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class GamificationController : ControllerBase
	{
		private readonly IUserRepository _userRepository;
		private readonly IGamificationRepository _gamificationRepository;


		public GamificationController(IUserRepository userRepository, IGamificationRepository gamificationRepository)
		{
			_userRepository = userRepository;
			_gamificationRepository = gamificationRepository;
		}
		// GET: api/<GamificationController>
		[HttpGet]
		public IEnumerable<string> Get()
		{
			return new string[] { "value1", "value2" };
		}

		// GET api/<GamificationController>/5
		[HttpGet("{id}")]
		public string Get(int id)
		{
			return "value";
		}

		// POST api/<GamificationController>
		[HttpPost]
		public void Post([FromBody] string value)
		{
		}

		// PUT api/<GamificationController>/5
		[HttpPut("{id}")]
		public void Put(int id, [FromBody] string value)
		{
		}

		[HttpPatch("{userid}/tokens")]
		public ActionResult PatchTokens(int userid, [FromBody] JObject data)
		{

			if(data["tokens"].ToString() == null)
			{
				return Ok(new { errorcode = Errors.ErrorCode.Invalid_Json_Object });
			}

			int value = Int32.Parse(data["tokens"].ToString());

			var user = _userRepository.GetUserWithLevelVip(userid);
			user.Level.Tokens += value;

			if(user.Level.Tokens < 0)
			{
				return Ok(new { errorcode = Errors.ErrorCode.Invalid_Amount_Of_Tokens });
			}

			return Ok(_userRepository.Save());
		}

		[HttpPatch("{userid}/experience")]
		public ActionResult PatchExperience(int userid, [FromBody] JObject data)
		{
			int value = Int32.Parse(data["experience"].ToString());

			if (data["experience"].ToString() == null)
			{
				return Ok(new { errorcode = Errors.ErrorCode.Invalid_Json_Object });
			}

			var user = _userRepository.GetUserWithLevelVip(userid);
			user.Level.Experience += value;

			return Ok(_userRepository.Save());
		}

		[HttpPatch("{userid}/weeklyexp")]
		public ActionResult PatchWeeklyExp(int userid, [FromBody] JObject data)
		{
			int value = Int32.Parse(data["weeklyexp"].ToString());

			if (data["weeklyexp"].ToString() == null)
			{
				return Ok(new { errorcode = Errors.ErrorCode.Invalid_Json_Object });
			}

			var user = _userRepository.GetUserWithLevelVip(userid);
			user.Level.WeeklyExp += value;

			return Ok(_userRepository.Save());
		}



		// DELETE api/<GamificationController>/5
		[HttpDelete("{id}")]
		public void Delete(int id)
		{
		}
	}
}