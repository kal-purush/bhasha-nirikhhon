using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Contoso.Shop.Web
{
    public class CartoesController : Controller
    {
        private readonly CartoesService cartoesService;

        public CartoesController(CartoesService cartoesService)
        {
            this.cartoesService = cartoesService;
        }

        [HttpPost]
        public IActionResult Index(string ccnumber, string cvv)
        {
           int result =  cartoesService.Salvar(ccnumber, cvv);

            return Ok(result);
        }
    }
}