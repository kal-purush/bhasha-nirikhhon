using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using PetOwner.Data;
using PetOwner.Models;
using PetOwner.Repository.Interfaces;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace PetOwner.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class ItemController : ControllerBase
	{
		private readonly IItemRepository _itemRepository;
		private readonly PetOwnerContext _context;

		public ItemController(IItemRepository itemRepository, PetOwnerContext context)
		{
			_itemRepository = itemRepository;
			_context = context;
		}

		// GET: api/<ItemController>
		[HttpGet]
		public IEnumerable<string> Get()
		{
			return new string[] { "value1", "value2" };
		}

		// GET api/<ItemController>/5
		[HttpGet("{id}")]
		public string Get(int id)
		{
			return "value";
		}

		// POST api/<ItemController>
		[HttpPost("{userid}")]	// add item
		public ActionResult Post(int userid, [FromBody] Item value)
		{
			var user = _context.Users.Where(x => x.UserId == userid).FirstOrDefault();

			if (user == null) return BadRequest();

			value.GroupId = user.GroupId;

			value.RecordStamp = DateTime.UtcNow;

			_itemRepository.Insert(value);

			if (_itemRepository.Save()) return Ok();

			return BadRequest();

		}

		// PUT api/<ItemController>/5
		[HttpPut("{id}")]
		public void Put(int id, [FromBody] string value)
		{
		}

		// DELETE api/<ItemController>/5
		[HttpDelete("{itemid}")] // delete item
		public ActionResult Delete(int itemid)
		{
			_itemRepository.Delete(_itemRepository.Get(itemid));

			_itemRepository.Save();

			return Ok();
		}
	}
}