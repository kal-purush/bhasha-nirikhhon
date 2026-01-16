using BLL.Entities;
using DAL;
using Microsoft.AspNetCore.Mvc;

namespace CatsCRUDApp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CatController : ControllerBase
    {
        private readonly AppDbContext _dbContext;
        public CatController(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        // GET api/Cat
        [HttpGet]
        public ActionResult Get()
        {
            return Ok(_dbContext.Cats);
        }

        // POST api/Cat
        [HttpPost]
        public ActionResult Post(Cat model)
        {
            if (!ModelState.IsValid) return BadRequest(ModelState);

            _dbContext.Cats.Add(model);
            _dbContext.SaveChanges();

            return Ok("Add successfully");
        }

        // Put api/Cat
        [HttpPut]
        public ActionResult Put(Cat model)
        {
            if (!ModelState.IsValid) return BadRequest(ModelState);


            var existingStudent = _dbContext.Cats.Where(s => s.Id == model.Id).FirstOrDefault();

            if (existingStudent != null)
            {
                existingStudent.Name = model.Name;

                _dbContext.SaveChanges();
            }
            else
            {
                return NotFound();
            }

            return Ok($"Object by {model.Id} id was updated successfully");
        }

        // DELETE api/Cat
        [HttpDelete]
        public ActionResult Delete(int? id)
        {
            if (!ModelState.IsValid) return BadRequest(ModelState);

            var existingStudent = _dbContext.Cats.Where(x => x.Id == id).FirstOrDefault();
            
            if (Equals(existingStudent, null)) return NotFound();

            _dbContext.Cats.Remove(existingStudent);
            _dbContext.SaveChanges();

            return Ok($"Object by {id} id  was removed successfully");
        }
    }
}