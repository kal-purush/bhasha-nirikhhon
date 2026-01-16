using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Description;
using ChoreScore.Models;

namespace ChoreScore.Controllers
{
    public class ChoresController : ApiController
    {
        private ApplicationDbContext db = new ApplicationDbContext();

        // GET: api/Chores
        public IQueryable<Chore> GetChores()
        {
            return db.Chores;
        }

        // GET: api/Chores/5
        [ResponseType(typeof(Chore))]
        public async Task<IHttpActionResult> GetChore(int id)
        {
            Chore chore = await db.Chores.FindAsync(id);
            if (chore == null)
            {
                return NotFound();
            }

            return Ok(chore);
        }

        // PUT: api/Chores/5
        [ResponseType(typeof(void))]
        public async Task<IHttpActionResult> PutChore(int id, ChoreViewModel chore)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != chore.Id)
            {
                return BadRequest();
            }

            var choreToEdit = db.Chores.Find(id);
            choreToEdit.ChoreName = chore.ChoreName;
            //...
            choreToEdit.user = db.Users.Find(chore.UserId);

            try
            {
                await db.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!ChoreExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }

            return StatusCode(HttpStatusCode.NoContent);
        }

        // POST: api/Chores
        [ResponseType(typeof(Chore))]
        public async Task<IHttpActionResult> PostChore(Chore chore)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            db.Chores.Add(chore);
            await db.SaveChangesAsync();

            return CreatedAtRoute("DefaultApi", new { id = chore.Id }, chore);
        }

        // DELETE: api/Chores/5
        [ResponseType(typeof(Chore))]
        public async Task<IHttpActionResult> DeleteChore(int id)
        {
            Chore chore = await db.Chores.FindAsync(id);
            if (chore == null)
            {
                return NotFound();
            }

            db.Chores.Remove(chore);
            await db.SaveChangesAsync();

            return Ok(chore);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }

        private bool ChoreExists(int id)
        {
            return db.Chores.Count(e => e.Id == id) > 0;
        }
    }
}