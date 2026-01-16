using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Bibliotheca.Data;
using Bibliotheca.Models;
using Bibliotheca.Backend.Query;
using Bibliotheca.Backend.Services;

namespace Bibliotheca.Backend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ImageDatasController : ControllerBase
    {
        private readonly BibliothecaContext _context;

        private readonly IObservationService _observationService;

        public ImageDatasController(BibliothecaContext context, IObservationService observationService)
        {
            _context = context;
            _observationService = observationService;
        }

        // GET: api/ImageDatas/5
        [HttpGet("{id}")]
        public async Task<ActionResult<ImageData>> GetImageData(Guid id)
        {
            var imageData = await _context.ImageData.FindAsync(id);

            if (imageData == null)
            {
                return NotFound();
            }

            return imageData;
        }

        // PUT: api/ImageDatas/5
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPut("{id}")]
        public async Task<IActionResult> PutImageData(Guid id, ImageData imageData)
        {
            if (id != imageData.Id)
            {
                return BadRequest();
            }

            _context.Entry(imageData).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!ImageDataExists(id))
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

        // POST: api/ImageDatas
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPost]
        public async Task<ActionResult<ImageData>> PostImageData(ImageDataAddToObservationQuery imageDataQuery)
        {
            var image = _observationService.GetImageDataFromQuery(imageDataQuery);
            _context.ImageData.Add(image);
            var parent = await _context.AnimalObservation.FindAsync(imageDataQuery.ParentId);
            if ( parent == null)
            {
                return NotFound();
            }
            if (parent.Images == null)
            {
                parent.Images = new List<ImageData>();
            }
            parent.Images.Add(image);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetImageData", new { id = image.Id }, image);
        }

        // DELETE: api/ImageDatas/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteImageData(Guid id)
        {
            var imageData = await _context.ImageData.FindAsync(id);
            if (imageData == null)
            {
                return NotFound();
            }

            _context.ImageData.Remove(imageData);
            await _context.SaveChangesAsync();

            return NoContent();
        }

        private bool ImageDataExists(Guid id)
        {
            return _context.ImageData.Any(e => e.Id == id);
        }
    }
}