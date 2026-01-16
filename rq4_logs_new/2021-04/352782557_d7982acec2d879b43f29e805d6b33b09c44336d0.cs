using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WayVidUI.Controllers
{

    [ApiController]
    [Route("[controller]")]
    public class ConfigController: ControllerBase
    {

        private IConfiguration appConfig;

        public ConfigController(IConfiguration appConfig)
        {
            this.appConfig = appConfig;
        }

        [HttpGet]
        public ConfigModel GetConfiguration()
        {
            return new ConfigModel
            {
                ApiURL = this.appConfig.GetValue<string>("ApiURL")
            };
        }
    }

    public class ConfigModel
    {
        public string ApiURL { get; set; }
    }
}