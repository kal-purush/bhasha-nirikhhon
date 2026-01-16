using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MinecraftSeeds.data;

namespace MinecraftSeeds.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ImageController : ControllerBase
    {
        private readonly MineCraftSeedContext _context;

        public ImageController(MineCraftSeedContext context)
        {
            _context = context;
        }

        // GET: api/Image
        [HttpGet]
        public async Task<ActionResult<IEnumerable<image>>> GetImages()
        {
            return await _context.Images.ToListAsync();
        }

        // GET: api/Image/5
        [HttpGet("{id}")]
        public async Task<ActionResult<image>> Getimage(int id)
        {
            var image = await _context.Images.FindAsync(id);

            if (image == null)
            {
                return NotFound();
            }

            return image;
        }

        // PUT: api/Image/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Putimage(int id, image image)
        {
            if (id != image.ImageId)
            {
                return BadRequest();
            }

            _context.Entry(image).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!imageExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }

            return NoContent();
        }

        // POST: api/Image
        [HttpPost]
        [Route("api/UploadImage")]
        public async Task<ActionResult<image>> Postimage(IFormFile file, image image)
        {
            try
            {
            var path = Path.Combine(Directory.GetCurrentDirectory(),"wwwroot/images");
            var stream = new FileStream(path, FileMode.Create);
                await file.CopyToAsync(stream);
            _context.Images.Add(image);
            await _context.SaveChangesAsync();
            }
            catch
            {
                return BadRequest();
            }
            return CreatedAtAction("Getimage", new { id = image.ImageId }, image);
        }

        // DELETE: api/Image/5
        [HttpDelete("{id}")]
        public async Task<ActionResult<image>> Deleteimage(int id)
        {
            var image = await _context.Images.FindAsync(id);
            if (image == null)
            {
                return NotFound();
            }

            _context.Images.Remove(image);
            await _context.SaveChangesAsync();

            return image;
        }

        private bool imageExists(int id)
        {
            return _context.Images.Any(e => e.ImageId == id);
        }
    }
}