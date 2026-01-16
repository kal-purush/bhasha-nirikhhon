using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LJGHistoryService.Models;
using LJGHistoryService.Tables;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace LJGHistoryService.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class ContactController : ControllerBase
    {
        private readonly string storageString = "DefaultEndpointsProtocol=https;AccountName=ljgwebsite;AccountKey=4+lge0bw2MN6o9Z4DssravCHaR1ZXuwN+1t26KM8Tb0w+gJeR90iQqFr6HQE/OCG+wjRrzx4+qU0eRLfjgtI6w==;EndpointSuffix=core.windows.net";

        [HttpPost]
        public bool Post([FromBody] ContactItem contactItem)
        {
            var x = contactItem;

            return true;

        }


    }
}