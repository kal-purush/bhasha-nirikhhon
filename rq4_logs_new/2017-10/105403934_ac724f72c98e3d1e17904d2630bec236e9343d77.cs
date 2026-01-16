using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Description;
using Collector2.DataContext;
using Collector2.Model;

namespace Collector2.DataService.Controllers
{
    public class ItemImagesController : ApiController
    {
        private CollectorContext db = new CollectorContext();

        // GET: api/ItemImages
        public IQueryable<ItemImage> GetItemImage()
        {
            return db.ItemImage;
        }

        // GET: api/ItemImages/5
        [ResponseType(typeof(ItemImage))]
        public IHttpActionResult GetItemImage(int id)
        {
            ItemImage itemImage = db.ItemImage.Find(id);
            if (itemImage == null)
            {
                return NotFound();
            }

            return Ok(itemImage);
        }

        // PUT: api/ItemImages/5
        [ResponseType(typeof(void))]
        public IHttpActionResult PutItemImage(int id, ItemImage itemImage)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != itemImage.ItemImageId)
            {
                return BadRequest();
            }

            db.Entry(itemImage).State = EntityState.Modified;

            try
            {
                db.SaveChanges();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!ItemImageExists(id))
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

        // POST: api/ItemImages
        [ResponseType(typeof(ItemImage))]
        public IHttpActionResult PostItemImage(ItemImage itemImage)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            db.ItemImage.Add(itemImage);
            db.SaveChanges();

            return CreatedAtRoute("DefaultApi", new { id = itemImage.ItemImageId }, itemImage);
        }

        // DELETE: api/ItemImages/5
        [ResponseType(typeof(ItemImage))]
        public IHttpActionResult DeleteItemImage(int id)
        {
            ItemImage itemImage = db.ItemImage.Find(id);
            if (itemImage == null)
            {
                return NotFound();
            }

            db.ItemImage.Remove(itemImage);
            db.SaveChanges();

            return Ok(itemImage);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }

        private bool ItemImageExists(int id)
        {
            return db.ItemImage.Count(e => e.ItemImageId == id) > 0;
        }
    }
}