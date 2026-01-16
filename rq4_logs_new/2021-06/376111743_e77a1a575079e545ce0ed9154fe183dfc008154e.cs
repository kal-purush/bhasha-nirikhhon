using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Web2ProjekatBackend.Models;
using Web2ProjekatBackend.Repository;

namespace Web2ProjekatBackend.Controllers
{
    public class UserInfoController : ApiController
    {
        IUserRepository _repo;

        public UserInfoController()
        {
            _repo = new UserRepository();
        }

        public IHttpActionResult Get(string username)
        {
            UserInfo uifo = _repo.GetUserInfoByUsername(username);
            if (uifo == null)
            {
                return NotFound();
            }
            return Ok(uifo);
        }

        public IHttpActionResult Post([FromBody] UserInfo uInfo)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _repo.PostUser(uInfo);
            return CreatedAtRoute("DefaultApi", new { id = uInfo.Id }, uInfo);
        }

    }
}