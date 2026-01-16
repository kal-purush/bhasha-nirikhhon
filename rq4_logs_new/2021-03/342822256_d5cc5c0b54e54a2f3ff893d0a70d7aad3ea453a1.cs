using HostelWebAPI.DataAccess.Interfaces;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace HostelWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PropertyTypesController : ControllerBase
    {
        private readonly IDbRepo repo;

        public PropertyTypesController(IDbRepo repo)
        {
            this.repo = repo;
        }

        public class PropertyTypeResponse
        {
            public PropertyTypeResponse(Models.PropertyType pt, int count)
            {
                Id = pt.PropertyTypeId;
                PropertyType = pt.Type;
                Description = pt.Description;
                ThumbnailImg = pt.ThumbnailImg;
                Count = count;
            }

            public string Id { get; set; }
            public string PropertyType { get; set; }
            public string Description { get; set; }
            public string ThumbnailImg { get; set; }
            public int Count { get; set; }
        }

        // GET: api/<PropertyTypesController>
        [HttpGet]
        public async Task<ActionResult<IEnumerable<PropertyTypeResponse>>> Get()
        {
            var types = await repo.PropertyTypes.GetAllAsync();
            var res = types.Select(t => new PropertyTypeResponse(t, repo.PropertyTypes.CountAsync(t.PropertyTypeId).Result)).ToList();
            return Ok(res);
        }
    }
}