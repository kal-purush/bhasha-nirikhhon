using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetTeam7API.Data;
using DotNetTeam7API.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DotNetTeam7API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MovieController : ControllerBase
    {
        private readonly MovieDbContext _db;

        public MovieController(MovieDbContext db)
        {
            _db = db;
        }

        [HttpGet]
        public ActionResult<IEnumerable<Movie>> GetAll()
        {
            try
            {
                var items = _db.Movies.ToList();
                if (!items.Any())
                    return NoContent();

                return Ok(items);
            }
            catch (Exception e)
            {
                return BadRequest();
            }

        }

        [HttpGet("{id}")]
        public ActionResult<Movie> GetById(int id)
        {
            try
            {
                var item = _db.Movies.Where(t => t.id == id).FirstOrDefault();
                if (item == null)
                    return NoContent();

                return Ok(item);
            }
            catch (Exception e)
            {
                return BadRequest();
            }
        }
    }
}