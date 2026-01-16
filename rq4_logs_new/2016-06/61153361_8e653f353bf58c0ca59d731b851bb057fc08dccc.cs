using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Cors;
using YogaEveryDamnDay.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;

// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace FinalCapstone.Controllers
{
    [Route("api/[controller]")]
    [Produces("application/json")]
    [EnableCors("AllowDevelopmentEnvironment")]
    public class PoseController : Controller
    {

        private YogaEveryDamnDayContext _context;

        public PoseController(YogaEveryDamnDayContext context)
        {
            _context = context;
        }

        // GET: api/pose/sanskrit
        [HttpGet]
        public IActionResult Get([FromQuery] string sanskrit)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            IQueryable<Pose> poses = from pose in _context.Pose
                                     select new Pose
                                     {
                                         PoseId = pose.PoseId,
                                         Sanskrit = pose.Sanskrit,
                                         CommonName = pose.CommonName,
                                         Description = pose.Description,
                                         Base64Image = pose.Base64Image,
                                         // FigurineHref = String.Format("/api/Inventory?GeekId={0}", pose.GeekId)
                                     };

            if (sanskrit != null)
            {
                poses = poses.Where(g => g.Sanskrit == sanskrit);
            }

            if (sanskrit == null)
            {
                return NotFound();
            }

            return Ok(poses);
        }

        // GET api/values/5
        [HttpGet("{id}", Name = "GetPose")]
        public IActionResult Get(int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            Pose poses = _context.Pose.Single(m => m.PoseId == id);

            if (poses == null)
            {
                return NotFound();
            }

            return Ok(poses);
        }

        //// GET api/values
        //[HttpGet]
        //public IActionResult Get()
        //{
        //    if (!ModelState.IsValid)
        //    {
        //        return BadRequest(ModelState);
        //    }

        //    Pose poses = _context.Pose(m => m.PoseId == id);

        //    if (poses == null)
        //    {
        //        return NotFound();
        //    }

        //    return Ok(poses);
        //}


        // POST api/values
        [HttpPost]
        public IActionResult Post([FromBody]Pose pose)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }


            var existingPose = from p in _context.Pose
                               where p.Sanskrit == pose.Sanskrit
                               select p;

            if (existingPose.Count<Pose>() > 0)
            {
                return new StatusCodeResult(StatusCodes.Status409Conflict);
            }


            _context.Pose.Add(pose);

            try
            {
                _context.SaveChanges();
            }
            catch (DbUpdateException)
            {
                if (PoseExists(pose.PoseId))
                {
                    return new StatusCodeResult(StatusCodes.Status409Conflict);
                }
                else
                {
                    throw;
                }
            }

            return CreatedAtRoute("GetPose", new { id = pose.PoseId }, pose);
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }







        private bool PoseExists(int id)
        {
            return _context.Pose.Count(e => e.PoseId == id) > 0;
        }



    }
}