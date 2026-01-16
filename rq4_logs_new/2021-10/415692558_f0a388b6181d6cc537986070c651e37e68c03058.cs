using Auction.LMS.CORE.DTO;
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
    public class RegisterController : Controller
    {
        private readonly IRegisterServices registerServices;
        public RegisterController(IRegisterServices registerServices)
        {
            this.registerServices = registerServices;
        }

        [HttpPost]
        public bool InsertRegister([FromBody] RegistorDTO registorDTO) { 
    
            return registerServices.InsertRegister(registorDTO);
        }

    }
}