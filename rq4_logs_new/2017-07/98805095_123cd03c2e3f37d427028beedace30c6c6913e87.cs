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
using BMSApplication.Models;

namespace BMSApplication.Controllers
{
    public class ColorsController : ApiController
    {
        private ApplicationDbContext db = new ApplicationDbContext();

        // GET: api/Colors
        public IQueryable<Color> GetColors()
        {
            return db.Colors;
        }

        // GET: api/Colors/5
        [ResponseType(typeof(Color))]
        public async Task<IHttpActionResult> GetColor(int id)
        {
            Color color = await db.Colors.FindAsync(id);
            if (color == null)
            {
                return NotFound();
            }

            return Ok(color);
        }

        // PUT: api/Colors/5
        [ResponseType(typeof(void))]
        public async Task<IHttpActionResult> PutColor(int id, Color color)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != color.Id)
            {
                return BadRequest();
            }

            db.Entry(color).State = EntityState.Modified;

            try
            {
                await db.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!ColorExists(id))
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

        // POST: api/Colors
        [ResponseType(typeof(Color))]
        public async Task<IHttpActionResult> PostColor(Color color)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            db.Colors.Add(color);
            await db.SaveChangesAsync();

            return CreatedAtRoute("DefaultApi", new { id = color.Id }, color);
        }

        // DELETE: api/Colors/5
        [ResponseType(typeof(Color))]
        public async Task<IHttpActionResult> DeleteColor(int id)
        {
            Color color = await db.Colors.FindAsync(id);
            if (color == null)
            {
                return NotFound();
            }

            db.Colors.Remove(color);
            await db.SaveChangesAsync();

            return Ok(color);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }

        private bool ColorExists(int id)
        {
            return db.Colors.Count(e => e.Id == id) > 0;
        }
    }
}