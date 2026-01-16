using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PTTGCSampleApp.Models;
using PTTGCSampleApp.Repository;

namespace PTTGCSampleApp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {

        private readonly IUserRepository _repository;

        public UserController(IUserRepository repository)
        {
            _repository = repository;
        }

        [HttpGet("{id}", Name = "Get")]
        public IActionResult Get(int id)
        {
            UserProfile user = _repository.GetUserByID(id);
            if (user != null)
            {
                return new OkObjectResult(user);
            }

            return new NotFoundObjectResult(null);
        }

        [HttpPost]
        public IActionResult Create([FromBody] UserProfile User)
        {
            UserProfile inserted = _repository.InsertUser(User);
            return new OkObjectResult(inserted);
        }

        [HttpGet()]
        public IActionResult List()
        {
            return new OkObjectResult(_repository.GetUsers());
        }
    }
}