using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventCatalogAPI.Data;
using EventCatalogAPI.Domain;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace EventCatalogAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CatalogController : ControllerBase
    {
        private readonly CatalogContext _context;
        private readonly IConfiguration _config;
        public CatalogController(CatalogContext context, IConfiguration config)
        {
            _context = context;
            _config = config;
        }

        //Getting Events in alphabetic order of Events Name 
        //with given Pagesize and PageIndex
        [HttpGet]
        [Route("[action]")]
        public async Task<IActionResult> Events(
            [FromQuery]int pageIndex = 0,
            [FromQuery]int pageSize = 6)
        {
            var eventsCount = await _context.Events.LongCountAsync();

            var events = await _context.Events.OrderBy(e => e.Name)
                .Skip(pageIndex * pageSize)
                .Take(pageSize)
                .ToListAsync();

            events = ChangePictureUrl(events);

            return Ok(events);
        }

        //Filtering Events according to Type, Category and Location

        [HttpGet]
        [Route("[action]/type/{EventTypeId}/category/{EventCategoryId}/location/{LoacationId}")]
        public async Task<IActionResult> Events(
            int? EventTypeId,
            int? EventCategoryId,
            int? LocationId,
           [FromQuery]int pageIndex = 0,
           [FromQuery]int pageSize = 6)
        {
            var root = (IQueryable<EventsCatalog>)_context.Events;
            if(EventTypeId.HasValue)
            {
                root = root.Where(e => e.EventTypeId == EventTypeId);
            }
            if (EventCategoryId.HasValue)
            {
                root = root.Where(e => e.EventCategoryId == EventCategoryId);
            }
            if (LocationId.HasValue)
            {
                root = root.Where(e => e.LocationId == LocationId);
            }

            var eventsCount = await root.LongCountAsync();

            var events = await root.OrderBy(e => e.Name)
                .Skip(pageIndex * pageSize)
                .Take(pageSize)
                .ToListAsync();

            events = ChangePictureUrl(events);

            return Ok(events);
        }
        // Changing Picture Url
        private List<EventsCatalog> ChangePictureUrl(List<EventsCatalog> events)
        {
            events.ForEach(
                c => c.PictureUrl = c.PictureUrl
                       .Replace("http://externalcatalogbaseurltobereplaced",
                       _config["ExternalCatalogBaseUrl"]));

            return events;
        }

        //Getting all the Event Types
        [HttpGet]
        [Route("[action]")]
        public async Task<IActionResult> EventTypes()
        {
            var events = await _context.EventTypes.ToListAsync();
            return Ok(events);
        }

        //Getting all the Event Categories
        [HttpGet]
        [Route("[action]")]
        public async Task<IActionResult> EventCategories()
        {
            var events = await _context.EventCategories.ToListAsync();
            return Ok(events);
            
        }

        //Getting all the Event Loacations
        [HttpGet]
        [Route("[action]")]
        public async Task<IActionResult> EventLocations()
        {
            var events = await _context.Locations.ToListAsync();
            return Ok(events);
        }


    }
}