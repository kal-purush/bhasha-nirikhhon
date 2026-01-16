using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApiEspinal.DbManager;
using ApiEspinal.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;


namespace ApiEspinal.Controllers
{
    [ApiController]
    [Route("api/Boleto")]
    public class BoletoController : ControllerBase
    {
        [HttpPost]
        public DataJson Get([FromBody] Boleto dataJson)
        {
            try
            {
                DataJson message = null;
                BoletodbManager boletodb = new BoletodbManager();
                message = boletodb.SaveBoleto(dataJson);
                return message;
            }
            catch
            {
                throw;
            }
        }
    }
}