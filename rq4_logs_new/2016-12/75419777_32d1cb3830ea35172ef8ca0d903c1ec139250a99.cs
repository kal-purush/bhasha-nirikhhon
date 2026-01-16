using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using EyesOnTheNet.DAL;
using EyesOnTheNet.Models;
using System.IdentityModel.Tokens.Jwt;


// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace EyesOnTheNet.Controllers
{
    [Route("api/[controller]")]
    public class CameraController : Controller
    {
        // GET: api/values
        [HttpGet]
        [Authorize]
        public IEnumerable<Camera> Get()
        {
            EyesOnTheNetRepository newRepo = new EyesOnTheNetRepository();
            string currentUser = new JwtSecurityToken(Request.Cookies["access_token"]).Subject;
            return newRepo.ReturnUserCameras(currentUser);
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            //EyesOnTheNetRepository newRepo = new EyesOnTheNetRepository();
            //newRepo.AddFakeUser();
            //newRepo.AddFakeCameras(id);
            return "Camaras Made!";
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody]string value)
        {
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
    }
}