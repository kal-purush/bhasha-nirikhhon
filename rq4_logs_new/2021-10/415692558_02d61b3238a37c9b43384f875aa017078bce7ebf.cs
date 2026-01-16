using Auction.LMS.CORE.Data;
using Auction.LMS.CORE.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Auction.LMS.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HomeController : ControllerBase
    {
        private readonly IHomeService homeService ;
        public HomeController(IHomeService homeService)
        {
            this.homeService = homeService;
        }

        [HttpGet]

        public List<Home> GetAllHome()
        {
            return homeService.GetAllHome();
        }

        [HttpPost]
        public bool InsertHome([FromBody] Home home)
        {
            return homeService.InsertHome(home);
        }
        [HttpPut]
        public bool UpdateHome(Home home)
        {
            return homeService.UpdateHome(home);
        }
        [HttpDelete("delete/{id}")]
        public bool DeleteHome(int id)
        {
            return homeService.DeleteHome(id);
        }
        [HttpGet]
        [Route("gethomebyid/{id}")]
        public Home GetHomeById(int id)
        {
            return homeService.GetHomeById(id);
        }
    }
}